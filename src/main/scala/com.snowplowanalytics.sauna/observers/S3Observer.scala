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
package com.snowplowanalytics.sauna.observers

import java.net.URLDecoder._
import scala.io.Source.fromInputStream

import awscala.s3.{Bucket, S3}
import awscala.sqs.{Queue, SQS}
import play.api.libs.json.Json

/**
  * Observes some AWS S3 bucket.
  */
class S3Observer(val s3: S3, val sqs: SQS, val queue: Queue) extends Observer {
  import S3Observer._

  /**
    * Gets file content from S3 bucket.
    */
  def getLines(bucketName: String, fileName: String): Seq[String] = {
    s3.get(Bucket(bucketName), fileName) match {
      case Some(o) => fromInputStream(o.content).getLines.toSeq
      case None => Seq()
    }
  }

  def watch(process: (Seq[String]) => Unit): Unit = {
    new Thread {
      override def run(): Unit = {
        while (true) {
          Thread.sleep(1000)
          sqs.receiveMessage(queue, count = 10) // blocking, so no overlapping happens
             .foreach { case message =>
               val (bucketName, fileName) = getBucketAndFile(message.body)
                                           .getOrElse(throw new Exception("Unable to find required fields in message json. Probably schema has changed."))
               val lines = getLines(bucketName, fileName)
               process(lines)
               sqs.delete(message)
             }
        }
      }
    }.start()
  }
}

object S3Observer {
  /**
    * Gets bucket and file names from given json.
    */
  def getBucketAndFile(messageJson: String): Option[(String, String)] = {
    val parsed = Json.parse(messageJson)
    val s3opt = (parsed \ "Records" \\ "s3").headOption

    for {
      s3 <- s3opt
      bucket <- (s3 \ "bucket" \ "name").asOpt[String]
      file <- (s3 \ "object" \ "key").asOpt[String]
    } yield (decode(bucket, "UTF-8"), file)
  }
}