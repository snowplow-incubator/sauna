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
import scala.concurrent.duration._
import scala.concurrent.{ Future, ExecutionContext }
import ExecutionContext.Implicits.global

// java
import java.net.URLDecoder

// akka
import akka.actor._

// play
import play.api.libs.json.{ Json, JsValue }

// amazonaws
import com.amazonaws.auth.{ BasicAWSCredentials, AWSCredentialsProvider, DefaultAWSCredentialsProviderChain }
import com.amazonaws.AbortedException
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration
import com.amazonaws.services.kinesis.model.Record

//kinesis
import com.gilt.gfc.aws.kinesis.client.{ KCLConfiguration, KinesisRecordReader, KCLWorkerRunner }

// sauna
import responders.Responder._
import Observer.{ KinesisRecordReceived }

class AmazonKinesisObserver(streamName: String, kclConfig: KinesisClientLibConfiguration) extends Actor with Observer {
  import AmazonKinesisObserver._

  override def preStart = {
    super.preStart()
    notify("Started Kinesis Observer")

    implicit object ARecordReader extends KinesisRecordReader[Record]{
      override def apply(r: Record) : Record = {
        r
      }
    }

    KCLWorkerRunner(kclConfig).runAsyncSingleRecordProcessor[Record](1 minute) { record: Record =>
      Future {
        notify(s"Got a Kinesis Record from ${streamName}")
        context.parent ! KinesisRecordReceived(streamName, record.getSequenceNumber(), record.getData(), self)

        record
      }
    }
  }

  def receive = {
    case _ =>
  }
}

object AmazonKinesisObserver {
  def props(streamName: String, kclConfig: KinesisClientLibConfiguration): Props =
    Props(new AmazonKinesisObserver(streamName, kclConfig))

  def props(parameters: AmazonKinesisConfigParameters): Props = {
    // AWS configuration. Safe to throw exception on initialization
    val credentialsProvider = if(parameters.awsSecretAccessKey.isEmpty) {
      new DefaultAWSCredentialsProviderChain()
    } else {
      val credentials = new BasicAWSCredentials(parameters.awsAccessKeyId.toString, parameters.awsAccessKeyId.toString)

      class KinesisCredentialsProvider(credentials: BasicAWSCredentials) extends AWSCredentialsProvider {
        def refresh() = { }
        def getCredentials = credentials
      }

      new KinesisCredentialsProvider(credentials)
    }

    val kclConfiguration = KCLConfiguration(
      parameters.applicationName,
      parameters.kinesisStreamName,
      credentialsProvider,
      credentialsProvider,
      credentialsProvider
    )

    props(parameters.kinesisStreamName, kclConfiguration)
  }
}
