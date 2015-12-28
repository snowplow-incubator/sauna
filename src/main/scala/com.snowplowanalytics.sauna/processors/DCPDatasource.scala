/*
 * Copyright (c) 2015 Snowplow Analytics Ltd. All rights reserved.
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

// java
import java.io.{File, InputStream, PrintWriter}
import java.text.{ParseException, SimpleDateFormat}
import java.util.UUID

// scala
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source.fromInputStream

// awscala
import awscala.Region
import awscala.s3.{Bucket, S3}

// sauna
import apis.Optimizely
import loggers.Notification

/**
 * Does stuff for Optimizely Dynamic Customer Profiles feature.
 */
class DCPDatasource(optimizely: Optimizely, saunaRoot: String, optimizelyImportRegion: String)
                   (implicit hasLogger: HasLogger) extends Processor {
  import DCPDatasource._
  import hasLogger.logger

  // todo tests everywhere after akka

  override def process(filePath: String, is: InputStream): Unit = filePath match {
    case pathRegexp(service, datasource, attrs) =>
      if (attrs.isEmpty) {
        logger ! Notification("Should be at least one attribute.")
        return
      }

      if (!attrs.contains("customerId")) {
        logger ! Notification("Attribute 'customerId' must be included.")
        return
      }

      optimizely.getOptimizelyS3Credentials(datasource)
                .foreach {
                  case Some((accessKey, secretKey)) =>
                    val correctedFile = correct(is, attrs) match {
                      case Some(file) =>
                        file
                      case None =>
                        logger ! Notification("Invalid file, stopping datasource uploading.")
                        return
                    }

                    implicit val region = Region.apply(optimizelyImportRegion)
                    implicit val s3 = S3(accessKey, secretKey)
                    val fileName = filePath.substring(filePath.indexOf(attrs) + attrs.length + 1)
                    val s3path = s"dcp/$service/$datasource/$fileName"

                    try {
                      Bucket("optimizely-import").put(s3path, correctedFile)
                      logger ! Notification(s"Successfully uploaded file to S3 bucket 'optimizely-import/$s3path'.")
                      if (!correctedFile.delete()) println(s"unable to delete file [$correctedFile].")

                    } catch { case e: Exception =>
                      logger ! Notification(e.getMessage)
                      logger ! Notification(s"Unable to upload to S3 bucket 'optimizely-import/$s3path'")
                    }

                  case None =>
                    logger ! Notification("Unable to get credentials for S3 bucket 'optimizely-import'.")
                }


    case _ => // do nothing
  }

  /**
   * Converts underlying source into Optimizely-friendly format.
   *
   * @see http://developers.optimizely.com/rest/customer_profiles/index.html#bulk
   * @see https://github.com/snowplow/sauna/wiki/Optimizely-responder-user-guide#2241-reformatting-for-the-bulk-upload-api
   * @param is An InputStream for some source.
   * @return Some(Corrected file) or None, if something (e.g. wrong date format) went wrong.
   */
  def correct(is: InputStream, header: String): Option[File] = {
    val sb = new StringBuilder(header + "\n")
    fromInputStream(is).getLines()
                       .foreach { case line =>
                         correct(line) match {
                           case Some(corrected) => sb.append(corrected + "\n")
                           case None => return None // notification is done in 'correct' method
                         }
                       }

    val _ = new File(saunaRoot + "/tmp/").mkdir() // if tmp/ does not exists
    val fileName = saunaRoot + "/tmp/" + UUID.randomUUID().toString
    val file = new File(fileName)

    new PrintWriter(fileName){
      write(sb.toString())
      close()
    }

    Some(file)
  }

  /**
   * Converts the line into Optimizely-friendly format.
   *
   * @see http://developers.optimizely.com/rest/customer_profiles/index.html#bulk
   * @see https://github.com/snowplow/sauna/wiki/Optimizely-responder-user-guide#2241-reformatting-for-the-bulk-upload-api
   * @param _line A string to be corrected.
   * @return Some(Corrected string) or None, if something (e.g. wrong date format) went wrong.
   */
  def correct(_line: String): Option[String] = {
    val line = _line.replaceAll("[ ]{2,}", "\t") // handle cases when \t got converted to spaces
    val sb = new StringBuilder
    var i = 0

    while (i < line.length) {
      val char = line(i)

      if (char == 't' && wordEnded(line, i + 1)) sb.append("true")
      else if (char == 'f' && wordEnded(line, i + 1)) sb.append("false")
      else if (char == '\t') sb.append(',')
      else if (char != '"') sb.append(char)

      i += 1
    }

    sb.toString() match {
      case dateRegexp(left, timestamp, right) =>
        try {
          val epoch = dateFormat.parse(timestamp)
                                .getTime
          Some(s"$left$epoch$right")

        } catch { case e: ParseException =>
          logger ! Notification(s"$timestamp is not in valid format. Try 'yyyy-MM-dd HH:mm:ss.SSS' .")
          None
        }

      case s => Some(s) // no timestamp. do nothing
    }
  }

  private def wordEnded(s: String, i: Int): Boolean = {
    if (i >= s.length) return false
    !s(i).isLetterOrDigit
  }
}

object DCPDatasource {
  val pathRegexp =
    """.*com\.optimizely/
      |dcp_datasource/
      |v1/
      |(.*?)/
      |(.*?)/
      |tsv:([^\/]+)/
      |.*$
    """.stripMargin
      .replaceAll("[\n ]", "")
      .r

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
  val dateRegexp = "(.*?)(\\d{1,4}-\\d{1,2}-\\d{1,2} \\d{1,2}:\\d{1,2}:\\d{1,2}\\.\\d{1,3})(.*)".r
}