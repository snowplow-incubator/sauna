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

// scala
import scala.util.control.NonFatal

// java
import java.net.URLDecoder

// akka
import akka.actor._

// play
import play.api.libs.json.{ Json, JsValue }

// awscala
import awscala.{ Region, Credentials }
import awscala.s3.{Bucket, S3}
import awscala.sqs.{Queue, SQS, Message}
import com.amazonaws.AbortedException

// sauna
import responders.Responder._
import Observer.{ DeleteS3Object, S3FilePublished }


/**
 * Observer awaiting for messages on SQS queue about newly created files on S3
 *
 * @param s3 AWS S3 client wrapper
 * @param sqs
 * @param queue
 */
class AmazonS3Observer(s3: S3, sqs: SQS, queue: Queue) extends Actor with Observer {
  import AmazonS3Observer._

  val monitor: AmazonSqsMonitor = new AmazonSqsMonitor(sqs, queue, forwardFilePublished, stopNotify)
  val monitorThread = new Thread(monitor)
  monitorThread.setDaemon(true)
  monitorThread.start()

  def receive: Receive = {
    case filePublished: S3FilePublished =>
      notify(s"Detected new S3 file [${filePublished.path}]")
      context.parent ! filePublished

    case deleteFile: DeleteS3Object =>
      deleteFile.run()
      deleteFile.deleteMessage(sqs)
  }

  /**
   * Callback to check the message is actually about created file, envelope it
   * and forward it to itself
   *
   * @param message SQS message received from monitor thread
   */
  def forwardFilePublished(message: Message): Unit = {
    getBucketAndFile(message) match {
      case Some((bucketName, fileName)) =>
        val decodedFileName = URLDecoder.decode(fileName, "UTF-8")
        val s3Source = S3Source(s3, Bucket(bucketName), message)
        self ! S3FilePublished(decodedFileName, s3Source, self)

      case None =>
        notify(s"Unknown message [$message] was published")
        sqs.delete(message)
    }
  }

  /**
   * Callback to notify system or recover from interrupted inner monitor thread
   *
   * @param throwable exception thrown by `Runnable`
   */
  def stopNotify(throwable: Throwable): Unit = {
    throwable match {
      case e: InterruptedException =>
        monitor.stop()
        notify("SqsMonitor thread has been stopped")
      case e: AbortedException =>   // Not sure why SQSClient throws it on actor shutdown
        monitor.stop()
        monitorThread.interrupt()
      case e => throw e
    }
  }

  /**
   * Kill monitor thread
   */
  override def postStop(): Unit = {
    monitor.stop()
    monitorThread.interrupt()
  }
}

object AmazonS3Observer {
  /**
   * Parse body of SQS message with notification about S3 event and extract
   * bucket and object path
   */
  def getBucketAndFile(message: Message): Option[(String, String)] = {
    val parsed = try {
      Some(Json.parse(message.body))
    } catch {
      case NonFatal(_) => None
    }

    parsed.flatMap(extractBucketAndFile)
  }

  /**
   * Helper function to extract and decode from SQS notification information
   * about created object
   *
   * @param body message's body parsed as JSON
   * @return some pair of bucket and path if message is correct
   */
  private[observers] def extractBucketAndFile(body: JsValue): Option[(String, String)] = {
    for {
      s3     <- (body \ "Records" \\ "s3").headOption
      bucket <- (s3 \ "bucket" \ "name").asOpt[String]
      file   <- (s3 \ "object" \ "key").asOpt[String]
    } yield (URLDecoder.decode(bucket, "UTF-8"), file)
  }

  def props(s3: S3, sqs: SQS, queue: Queue): Props =
    Props(new AmazonS3Observer(s3, sqs, queue))

  def props(parameters: AmazonS3ConfigParameters): Props = {
    // AWS configuration. Safe to throw exception on initialization
    val region = Region(parameters.awsRegion)
    val credentials = new Credentials(parameters.awsAccessKeyId, parameters.awsSecretAccessKey)
    val s3 = S3(credentials)(region)
    val sqs = SQS(credentials)(region)
    val queue = sqs.queue(parameters.sqsQueueName)
      .getOrElse(throw new RuntimeException(s"No queue [${parameters.sqsQueueName}] found"))
    props(s3, sqs, queue)
  }
}
