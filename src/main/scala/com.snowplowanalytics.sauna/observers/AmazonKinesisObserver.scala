/*
 * Copyright (c) 2016-2017 Snowplow Analytics Ltd. All rights reserved.
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

// java
import java.util.Date

// scala
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

// akka
import akka.actor._

// amazonaws
import com.amazonaws.auth.{AWSCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration}
import com.amazonaws.services.kinesis.model.Record

// kinesis
import com.gilt.gfc.aws.kinesis.client.{KCLConfiguration, KCLWorkerRunner, KinesisRecordReader}

// sauna
import Observer.KinesisRecordReceived

class AmazonKinesisObserver(streamName: String, kclConfig: KinesisClientLibConfiguration) extends Actor with Observer {
  override def preStart: Unit = {
    super.preStart()
    notify("Started Kinesis Observer")

    /**
     * [[KinesisRecordReader]] instance that will return processed records
     * without any transformation.
     */
    implicit object ARecordReader extends KinesisRecordReader[Record] {
      override def apply(r: Record): Record = r
    }

    /**
     * Start a worker thread that will notify the mediator about processed
     * Kinesis records.
     */
    KCLWorkerRunner(kclConfig).runAsyncSingleRecordProcessor[Record](1 minute) { record: Record =>
      Future {
        notify(s"Received Kinesis Record from $streamName")
        context.parent ! KinesisRecordReceived(streamName, record.getSequenceNumber, record.getData, self)
      }
    }
  }

  def receive: Receive = {
    case _ =>
  }
}

object AmazonKinesisObserver {
  def props(streamName: String, kclConfig: KinesisClientLibConfiguration): Props =
    Props(new AmazonKinesisObserver(streamName, kclConfig))

  def props(config: AmazonKinesisConfig_1_0_0): Props = {

    // AWS configuration. Safe to throw exception on initialization
    val credentials = new BasicAWSCredentials(
      config.parameters.aws.accessKeyId,
      config.parameters.aws.secretAccessKey)

    class KinesisCredentialsProvider(credentials: BasicAWSCredentials) extends AWSCredentialsProvider {
      def refresh(): Unit = {}

      def getCredentials: BasicAWSCredentials = credentials
    }

    val credentialsProvider = new KinesisCredentialsProvider(credentials)

    configBuilder(credentialsProvider, config) match {
      case Some(kclConfiguration) => props(config.parameters.kinesis.streamName, kclConfiguration)
      case None => Props.empty
    }
  }

  def configBuilder(
    credentialsProvider: AWSCredentialsProvider,
    amazonKinesisConfig: AmazonKinesisConfig_1_0_0): Option[KinesisClientLibConfiguration] = {
    val initialConfiguration = KCLConfiguration(
      amazonKinesisConfig.id,
      amazonKinesisConfig.parameters.kinesis.streamName,
      credentialsProvider,
      credentialsProvider,
      credentialsProvider
    )
      .withRegionName(amazonKinesisConfig.parameters.kinesis.region)
      .withMaxRecords(amazonKinesisConfig.parameters.kinesis.maxRecords)

    amazonKinesisConfig.parameters.kinesis.initialPosition match {
      case ShardIteratorType_1_0_0.LATEST =>
        Some(initialConfiguration.withInitialPositionInStream(InitialPositionInStream.LATEST))
      case ShardIteratorType_1_0_0.TRIM_HORIZON =>
        Some(initialConfiguration.withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON))
      case ShardIteratorType_1_0_0.AT_TIMESTAMP =>
        (for {
          modifiers <- amazonKinesisConfig.parameters.kinesis.initialPositionModifiers
          timestamp <- modifiers.timestamp
        } yield timestamp) match {
          case Some(ts) => Some(initialConfiguration
            .withInitialPositionInStream(InitialPositionInStream.AT_TIMESTAMP)
            .withTimestampAtInitialPositionInStream(new Date(ts)))
          case None => None
        }
    }
  }
}