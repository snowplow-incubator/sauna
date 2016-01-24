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
package processors
package optimizely

// java
import java.io.{File, InputStream, PrintWriter, StringReader}
import java.text.SimpleDateFormat
import java.util.UUID

// scala
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source.fromInputStream

// akka
import akka.actor.{ActorRef, ActorSystem, Props}

// awscala
import awscala.Region
import awscala.s3.{Bucket, S3}

// scala-csv
import com.github.tototoshi.csv._

// sauna
import apis.Optimizely
import loggers.Logger.Notification
import processors.Processor.FileAppeared

/**
 * Does stuff for Optimizely Dynamic Customer Profiles feature.
 *
 * @see https://github.com/snowplow/sauna/wiki/Optimizely-responder-user-guide#dcp-batch
 * @param optimizely Instance of Optimizely.
 * @param saunaRoot A place for 'tmp' directory.
 * @param optimizelyImportRegion What region uses Optimizely S3 bucket.
 * @param logger A logger actor.
 */
class DCPDatasource(optimizely: Optimizely, saunaRoot: String, optimizelyImportRegion: String)
                   (implicit logger: ActorRef) extends Processor {
  import DCPDatasource._

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

  override def process(fileAppeared: FileAppeared): Unit = {
    import fileAppeared._

    filePath match {
      case pathRegexp(service, datasource, attrs) =>
        if (attrs.isEmpty) {
          logger ! Notification("Should be at least one attribute.")
        }

        if (!attrs.contains("customerId")) {
          logger ! Notification("Attribute 'customerId' must be included.")
        }

        optimizely.getOptimizelyS3Credentials(datasource)
                  .foreach {
                    case Some((accessKey, secretKey)) =>
                      val correctedFile = correct(is, attrs)
                      implicit val region = Region.apply(optimizelyImportRegion)
                      implicit val s3 = S3(accessKey, secretKey)
                      val fileName = filePath.substring(filePath.indexOf(attrs) + attrs.length + 1)
                      val s3path = s"dcp/$service/$datasource/$fileName"

                      try {
                        Bucket("optimizely-import").put(s3path, correctedFile) // trigger Optimizely to get data from this bucket
                        logger ! Notification(s"Successfully uploaded file to S3 bucket 'optimizely-import/$s3path'.")
                        if (!correctedFile.delete()) println(s"unable to delete file [$correctedFile].")

                      } catch { case e: Exception =>
                        println(e.getMessage)
                        logger ! Notification(e.getMessage)
                        logger ! Notification(s"Unable to upload to S3 bucket 'optimizely-import/$s3path'")
                      }

                    case None =>
                      logger ! Notification("Unable to get credentials for S3 bucket 'optimizely-import'.")
                  }
    }
  }

  /**
   * Converts underlying source into Optimizely-friendly format.
   *
   * @see http://developers.optimizely.com/rest/customer_profiles/index.html#bulk
   * @see https://github.com/snowplow/sauna/wiki/Optimizely-responder-user-guide#2241-reformatting-for-the-bulk-upload-api
   * @param is An InputStream for some source.
   * @return Corrected file.
   */
  def correct(is: InputStream, header: String): File = {
    val sb = new StringBuilder(header + "\n")
    fromInputStream(is).getLines()
                       .foreach { case line => correct(line) match {
                           case Some(corrected) => sb.append(corrected + "\n")
                           case _ => sb.append("\n")
                         }
                       }

    val _ = new File(saunaRoot + "/tmp/").mkdir() // if tmp/ does not exists
    val fileName = saunaRoot + "/tmp/" + UUID.randomUUID().toString
    val file = new File(fileName)

    new PrintWriter(fileName){
      write(sb.toString())
      close()
    }

    file
  }

  /**
   * Converts the line into Optimizely-friendly format.
   *
   * @see http://developers.optimizely.com/rest/customer_profiles/index.html#bulk
   * @see https://github.com/snowplow/sauna/wiki/Optimizely-responder-user-guide#2241-reformatting-for-the-bulk-upload-api
   * @param line A string to be corrected.
   * @return Some(Corrected string) or None, if something (e.g. wrong date format) went wrong.
   */
  def correct(line: String): Option[String] = {
    val reader = CSVReader.open(new StringReader(line))(tsvFormat)

    try {
      reader.readNext() // get next line, it should be only one
            .map(list => list.map(correctWord)
                             .mkString(",")
                             .replaceAll("\"", ""))

    } catch {
      case _: Exception => None

    } finally {
      reader.close()
    }
  }

  /**
   * Corrects a single word according to following rules:
   *   1) Change "t" to "true"
   *   2) Change "f" to "false"
   *   3) Change timestamp to epoch
   *
   * @see https://github.com/snowplow/sauna/wiki/Optimizely-responder-user-guide#2241-reformatting-for-the-bulk-upload-api
   * @param word A word to be corrected.
   * @return Corrected word.
   */
  def correctWord(word: String): String = word match {
    case dateRegexp(timestamp) => dateFormat.parse(timestamp)
                                            .getTime
                                            .toString
    case "t" => "true"
    case "f" => "false"
    case _ => word
  }
}

object DCPDatasource {
  val tsvFormat = new TSVFormat {} // force scala-csv to use tsv
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
  val dateRegexp = "^(\\d{1,4}-\\d{1,2}-\\d{1,2} \\d{1,2}:\\d{1,2}:\\d{1,2}\\.\\d{1,3})$".r

  /**
   * Constructs a DCPDatasource actor.
   *
   * @param optimizely Instance of Optimizely.
   * @param saunaRoot A place for 'tmp' directory.
   * @param optimizelyImportRegion What region uses Optimizely S3 bucket.
   * @param system Actor system that creates an actor.
   * @param logger Actor with underlying Logger.
   * @return DCPDatasource as ActorRef.
   */
  def apply(optimizely: Optimizely, saunaRoot: String, optimizelyImportRegion: String)
           (implicit system: ActorSystem, logger: ActorRef): ActorRef =
    system.actorOf(Props(new DCPDatasource(optimizely, saunaRoot, optimizelyImportRegion)))
}