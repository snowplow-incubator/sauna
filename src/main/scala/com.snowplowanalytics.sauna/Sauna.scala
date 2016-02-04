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

// java
import java.io.File

// akka
import akka.actor.{Props, ActorSystem}

// awscala
import awscala.dynamodbv2.DynamoDB
import awscala.s3.S3
import awscala.sqs.SQS
import awscala.{Credentials, Region}

// sauna
import loggers._
import observers._
import responders.optimizely._
import responders.sendgrid._
import apis._

/**
 * Main class, starts the Sauna program.
 */
object Sauna extends App {
  if (args.length != 1) {
    println("""Usage: 'sbt "run <path_to_file_with_credentials>"' """)
    System.exit(1)
  }

  println("Starting application... \n")

  implicit val system = ActorSystem("sauna")

  // configuration
  val config = SaunaConfig(new File(args(0)))
  implicit val region = Region(config.awsRegion)
  implicit val credentials = new Credentials(config.accessKeyId, config.secretAccessKey)

  // loggers
  implicit val logger = system.actorOf(Props(new StdoutLogger))

  // S3
  val s3 = S3(credentials)

  // DynamoDB
  val ddb = DynamoDB(credentials)
  val ddbTable = ddb.table(config.ddbTableName)
                    .getOrElse(throw new Exception("No queue with that name found"))

  // SQS
  val sqs = SQS(credentials)
  val queue = sqs.queue(config.queueName)
                 .getOrElse(throw new Exception("No queue with that name found"))

  // apis
  val optimizely = new Optimizely(config.optimizelyToken)
  val sendgrid = new Sendgrid(config.sendgridToken)

  // responders
  val responderActors = Seq(
    TargetingList(optimizely),
    DCPDatasource(optimizely, config.saunaRoot, config.optimizelyImportRegion),
    Recipients(sendgrid)
  )

  // define and run observers
  val observers = Seq(
    new LocalObserver(config.saunaRoot, responderActors),
    new S3Observer(s3, sqs, queue, responderActors)
  )
  observers.foreach(new Thread(_).start())

  println("Application started. \n")
}