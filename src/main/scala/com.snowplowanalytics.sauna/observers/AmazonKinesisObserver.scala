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
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

// akka
import akka.actor._

// amazonaws
import com.amazonaws.auth.{AWSCredentialsProvider, BasicAWSCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration
import com.amazonaws.services.kinesis.model.Record

// kinesis
import com.gilt.gfc.aws.kinesis.client.{KCLConfiguration, KCLWorkerRunner, KinesisRecordReader}

// sauna
import com.snowplowanalytics.sauna.observers.Observer.KinesisRecordReceived

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
        notify(s"Received Kinesis Record from $streamName")
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
      val credentials = new BasicAWSCredentials(parameters.awsAccessKeyId, parameters.awsSecretAccessKey)

      class KinesisCredentialsProvider(credentials: BasicAWSCredentials) extends AWSCredentialsProvider {
        def refresh() = { }
        def getCredentials = credentials
      }

      new KinesisCredentialsProvider(credentials)
    }

    val kclConfiguration = if(parameters.awsRegion.isEmpty) {
      KCLConfiguration(
        parameters.applicationName,
        parameters.kinesisStreamName,
        credentialsProvider,
        credentialsProvider,
        credentialsProvider
      )
    } else {
      KCLConfiguration(
        parameters.applicationName,
        parameters.kinesisStreamName,
        credentialsProvider,
        credentialsProvider,
        credentialsProvider
      ).withRegionName(parameters.awsRegion)
    }

    props(parameters.kinesisStreamName, kclConfiguration)
  }
}
