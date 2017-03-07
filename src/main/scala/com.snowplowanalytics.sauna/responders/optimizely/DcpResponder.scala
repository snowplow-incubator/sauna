/*
 * Copyright (c) 2016 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.sauna
package responders
package optimizely

// java
import java.io.{File, InputStream, PrintWriter, StringReader}
import java.util.UUID

// nscala-time
import com.github.nscala_time.time.StaticDateTimeFormat

// scala
import scala.concurrent.Future
import scala.io.Source.fromInputStream
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

// akka
import akka.actor.{ActorRef, Props}

// awscala
import awscala.Region
import awscala.s3.{Bucket, S3}

// scala-csv
import com.github.tototoshi.csv._

// sauna
import DcpResponder._
import apis.Optimizely
import observers.Observer._
import responders.Responder._
import utils._


/**
 * Does stuff for Optimizely Dynamic Customer Profiles feature.
 *
 * @see https://github.com/snowplow/sauna/wiki/Optimizely-responder-user-guide#dcp-batch
 * @param optimizely   instance of Optimizely API Wrapper
 * @param importRegion AWS region for Optimizely S3 bucket
 * @param logger       A logger actor
 */
class DcpResponder(optimizely: Optimizely, importRegion: String, val logger: ActorRef) extends Responder[ObserverFileEvent, CustomersProfilesPublished] {

  import context.dispatcher

  /**
   * Extract [[CustomersProfilesPublished]] from path like:
   * `com.optimizely.dcp/datasource/v1/4034482827/5080511852/tsv:isVip,customerId,spendSegment,whenCreated/ua-team/joe/warehouse.tsv`
   * Where attributes after `tsv:` should denote column names in `warehouse.tsv`
   *
   * @param observerEvent some event sent from observer
   * @return Some [[CustomersProfilesPublished]] if this observer-event need to be
   *         processed by this responder, None if event need to be skept
   */
  def extractEvent(observerEvent: ObserverEvent): Option[CustomersProfilesPublished] = {
    observerEvent match {
      case e: ObserverFileEvent =>
        extractCustomerPublished(e) match {
          case Right(event) => Some(event)
          case Left(Some(error)) =>
            // TODO: cases where path is only partly correct should be handled differently, without deleting source file
            notify(error)
            None
          case Left(None) =>
            None
        }
      case _ => None
    }
  }

  /**
   * Extract credentials from DCP datastore, correct published file and upload
   * corrected file to Optimizely's private `/optimizely-import` S3 bucket
   * using extracted credentials
   */
  def process(event: CustomersProfilesPublished): Unit = {
    optimizely.getOptimizelyS3Credentials(event.datasource)
      .flatMap {
        case Some((accessKey, secretKey)) =>
          val s3 = S3(accessKey, secretKey)(Region(importRegion))
          prepareFile(event) match {
            case Some(prepared) => publishFile(prepared, s3)
            case None => Future.failed(new RuntimeException(s"Cannot read file [${event.source.id}]"))
          }
        case None => Future.failed(new RuntimeException(s"Cannot get AWS credentials for datasource [${event.datasource}]"))
      }
      .onComplete {
        case Success(message) => context.parent ! CustomersProfilesUploaded(event, message)
        case Failure(error) => notify(error.toString)
      }
  }

  /**
   * Perform S3 upload of transformed temporary result file, which triggers
   * Optimizely to import profiles. When upload finishers - delete file
   *
   * @param resultFile corrected file with S3 path and content
   * @param s3         client object with credentials and region
   * @return message about successful upload. S3 throws exceptions, so on
   *         failed upload - it will be failed `Future`
   */
  def publishFile(resultFile: ResultFile, s3: S3): Future[String] = {
    val upload = Future {
      Bucket("optimizely-import").put(resultFile.key, resultFile.file)(s3)
    }
    upload.onComplete { case _ => resultFile.file.delete() }
    upload.map(_ => s"Successfully uploaded file to S3 bucket '/optimizely-import/${resultFile.key}")
  }
}

object DcpResponder {

  val dateFormat = StaticDateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").withZoneUTC()
  val dateRegexp = "^(\\d{1,4}-\\d{1,2}-\\d{1,2} \\d{1,2}:\\d{1,2}:\\d{1,2}\\.\\d{1,3})$".r

  val pathPattern =
    """.*com\.optimizely\.dcp/
      |datasource/
      |v1/
      |(.*?)/
      |(.*?)/
      |tsv:([^\/]+)/
      |.+$
    """.stripMargin
      .replaceAll("[\n ]", "")

  val pathRegexp = pathPattern.r

  private val tmpdir = System.getProperty("java.io.tmpdir", "/tmp")

  /**
   * Responder event, denoting that Dynamic Customer Profiles TSV file were published
   *
   * @param service    DCP Service Id. A DCP Service collects, stores, and processes the customer data
   * @param datasource DCP datasource Id. A datasource stores a set of related customer attributes under a common ID space
   * @param attrs      comma-separated DCP attributes
   * @param source     observer event triggered this responder event
   */
  case class CustomersProfilesPublished(
    service: String,
    datasource: String,
    attrs: String,
    source: ObserverFileEvent
  ) extends ResponderEvent

  case class CustomersProfilesUploaded(
    source: CustomersProfilesPublished,
    message: String
  ) extends ResponderResult

  /**
   * Result file that has been corrected, transformed and reaady to be
   * uploaded into S3 bucket
   *
   * @param key  path on S3
   * @param file content of file
   */
  case class ResultFile(key: String, file: File)

  /**
   * Convert [[CustomersProfilesPublished]] event to a file ready to be uploaded to S3
   *
   * @param customersProfilesPublished event containing all input data
   * @return pair of content and path to S3
   */
  def prepareFile(customersProfilesPublished: CustomersProfilesPublished): Option[ResultFile] = {
    import customersProfilesPublished._
    source.streamContent match {
      case Some(content) =>
        val correctedFile = correct(content, attrs)
        val fileName = correctName(customersProfilesPublished)
        val s3path = s"dcp/$service/$datasource/$fileName"
        Some(ResultFile(s3path, correctedFile))
      case None => None
    }
  }

  /**
   * Remove attributes, leaving only subpath and add ".csv" extension
   *
   * @param source original event of published file
   * @return corrected file path
   */
  def correctName(source: CustomersProfilesPublished): String = {
    val original = source.source.id.substring(source.source.id.indexOf(source.attrs) + source.attrs.length + 1)
    if (original.endsWith(".tsv")) {
      original.stripSuffix(".tsv") + ".csv"
    } else {
      original + ".csv"
    }
  }

  /**
   * Converts underlying stream of bytes into Optimizely-friendly CSV format
   *
   * @see http://developers.optimizely.com/rest/customer_profiles/index.html#bulk
   * @see https://github.com/snowplow/sauna/wiki/Optimizely-responder-user-guide#2241-reformatting-for-the-bulk-upload-api
   * @param is An InputStream for some source.
   * @return Corrected file.
   */
  def correct(is: InputStream, header: String): File = {
    val sb = new StringBuilder(header + "\n")
    fromInputStream(is).getLines.map(correctLine).foreach {
      case Some(corrected) => sb.append(corrected + "\n")
      case _ => sb.append("\n")
    }

    val tmpfile = new File(tmpdir, "correct-" + UUID.randomUUID().toString)

    new PrintWriter(tmpfile) {
      writer =>
      writer.write(sb.toString)
      writer.close()
    }

    tmpfile
  }

  /**
   * Converts the line into Optimizely-friendly format
   *
   * @see http://developers.optimizely.com/rest/customer_profiles/index.html#bulk
   * @see https://github.com/snowplow/sauna/wiki/Optimizely-responder-user-guide#2241-reformatting-for-the-bulk-upload-api
   * @param line A string to be corrected.
   * @return Some(Corrected string) or None, if something (e.g. wrong date format) went wrong.
   */
  def correctLine(line: String): Option[String] = {
    val reader = CSVReader.open(new StringReader(line))(tsvFormat)
    try {
      reader.readNext() // get next line, it should be only one
        .map(_.map(correctWord).mkString(",").stripPrefix("\"").stripSuffix("\""))
    } catch {
      case NonFatal(e) => None
    } finally {
      reader.close()
    }
  }

  /**
   * Corrects a single word according to following rules:
   * 1) Change "t" to "true"
   * 2) Change "f" to "false"
   * 3) Change timestamp to epoch
   *
   * @see https://github.com/snowplow/sauna/wiki/Optimizely-responder-user-guide#2241-reformatting-for-the-bulk-upload-api
   * @param word A word to be corrected.
   * @return Corrected word.
   */
  @throws[IllegalArgumentException]
  private def correctWord(word: String): String = word match {
    case dateRegexp(timestamp) =>
      dateFormat.parseDateTime(timestamp).getMillis.toString
    case "t" => "true"
    case "f" => "false"
    case _ => word
  }

  /**
   * Actor-independent implementation of `extractEvent`.
   * Extract [[CustomersProfilesPublished]] from path like:
   * `com.optimizely.dcp/datasource/v1/4034482827/5080511852/tsv:isVip,customerId,spendSegment,whenCreated/ua-team/joe/warehouse.tsv`
   * Where attributes after `tsv:` should denote column names in `warehouse.tsv`
   *
   * @param observerEvent file published event sent by observer
   * @return right responder event if it can be extracted from filepath,
   *         left some error if path is correct only partly
   *         left none if file shouldn't be handled by responder
   */
  def extractCustomerPublished(observerEvent: ObserverFileEvent): Either[Option[String], CustomersProfilesPublished] = {
    observerEvent.id match {
      case pathRegexp(service, datasource, attrs) if attrs.contains("customerId") =>
        Right(CustomersProfilesPublished(service, datasource, attrs, observerEvent))
      case pathRegexp(_, _, _) =>
        Left(Some(s"DcpResponder: attribute 'customerId' for [${observerEvent.id}] must be included"))
      case _ => Left(None)
    }
  }

  /**
   * Constructs a Props for DcpResponder actor.
   *
   * @param optimizely             Instance of Optimizely
   * @param optimizelyImportRegion what region uses Optimizely S3 bucket
   * @param logger                 Actor with underlying Logger
   * @return Props for new actor
   */
  def props(optimizely: Optimizely, optimizelyImportRegion: String, logger: ActorRef): Props =
    Props(new DcpResponder(optimizely, optimizelyImportRegion, logger))
}