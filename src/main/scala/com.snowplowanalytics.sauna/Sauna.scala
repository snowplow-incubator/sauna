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

// java
import java.io.File

// awscala
import awscala.dynamodbv2.DynamoDB
import awscala.s3.S3
import awscala.sqs.SQS
import awscala.{Credentials, Region}

// sauna
import com.snowplowanalytics.sauna.loggers._
import com.snowplowanalytics.sauna.observers._
import com.snowplowanalytics.sauna.processors._
import com.snowplowanalytics.sauna.apis._

/**
 * Main class, starts the Sauna program.
 */
object Sauna extends App {
  if (args.length != 1) {
    println("""Usage: 'sbt "run <path_to_file_with_credentials>"' """)
    System.exit(1)
  }

  // configuration
  val saunaConfig = SaunaConfig(new File(args(0)))
  implicit val region = Region.US_WEST_2
  implicit val credentials = new Credentials(saunaConfig.accessKeyId, saunaConfig.secretAccessKey)

  // S3
  val s3 = S3(credentials)

  // DynamoDB
  val ddb = DynamoDB(credentials)
  val ddbTable = ddb.table(saunaConfig.ddbTableName)
                    .getOrElse(throw new Exception("No queue with that name found"))

  // SQS
  val sqs = SQS(credentials)
  val queue = sqs.queue(saunaConfig.queueName)
                 .getOrElse(throw new Exception("No queue with that name found"))

  // responders
  val optimizely = new Optimizely with StdoutLogger
//  val optimizely = new Optimizely with HipchatLogger with DDBLogger

  // processors
  val processors = Seq(
    new TargetingList(optimizely) with StdoutLogger,
    new DCPDatasource(optimizely, saunaConfig.saunaRoot) with StdoutLogger
//    new TargetingList(optimizely) with HipchatLogger with DDBLogger,
//    new DCPDatasource(optimizely, saunaConfig.saunaRoot) with HipchatLogger with DDBLogger
  )

  // define and run observers
  val observers = Seq(
    new LocalObserver(saunaConfig.saunaRoot, processors) with StdoutLogger,
    new S3Observer(s3, sqs, queue, processors) with StdoutLogger
//    new LocalObserver(saunaConfig.saunaRoot, processors) with HipchatLogger with DDBLogger,
//    new S3Observer(s3, sqs, queue, processors) with HipchatLogger with DDBLogger
  ).foreach(o => new Thread(o).start())
}