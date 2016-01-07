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
package observers

//java
import java.io.InputStream
import java.net.URLDecoder

// akka
import akka.actor.ActorRef

// play
import play.api.libs.json.Json

// awscala
import awscala.s3.{Bucket, S3}
import awscala.sqs.{Queue, SQS}

// sauna
import loggers.Logger.Notification
import processors.Processor._

/**
 * Observes some AWS S3 bucket.
 *
 * @param s3 Provides actions for AWS S3.
 * @param sqs Both with `queue` provide actions for AWS SQS.
 * @param queue Both with `sqs` provide actions for AWS SQS.
 * @param processors A Seq of ActorRef with underlying Processor. They will be called after new file appeared.
 * @param logger A logger actor.
 */
class S3Observer(s3: S3, sqs: SQS, queue: Queue, processors: Seq[ActorRef])
                (implicit logger: ActorRef) extends Observer {
  import S3Observer._

  /**
   * Gets file content from S3 bucket.
   */
  def getInputStream(bucketName: String, fileName: String): InputStream = {
    s3.get(Bucket(bucketName), fileName)
      .map(_.content)
      .getOrElse( // easier to return empty one than wrap whole stuff in Option-like
        new InputStream {
          override def read(): Int = -1
        }
      )
  }

  override def run(): Unit = {
    while (true) {
      Thread.sleep(1000)
      sqs.receiveMessage(queue, count = 10) // blocking, so no overlapping happens
         .foreach { case message =>
           val (bucketName, fileName) = getBucketAndFile(message.body)
                                         .getOrElse(throw new Exception("Unable to find required fields in message json. Probably schema has changed."))
           val decodedFileName = URLDecoder.decode(fileName, "UTF-8")
           val is = getInputStream(bucketName, decodedFileName)

           logger ! Notification(s"Detected new S3 file $decodedFileName.")
           processors.foreach(_ ! FileAppeared(decodedFileName, is, InS3(s3, bucketName, fileName)))
           sqs.delete(message)
         }
    }
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
    } yield (URLDecoder.decode(bucket, "UTF-8"), file)
  }
}