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

import java.io.{InputStream, File}
import scala.io.Source.fromInputStream

import awscala.{Region, Credentials}
import awscala.s3.S3
import awscala.sqs.SQS

import com.snowplowanalytics.sauna.responders.optimizely._
import com.snowplowanalytics.sauna.observers._

/**
  * Main class, starts the Sauna program.
  */
object Sauna extends App {
  if (args.length != 1) {
    println("""Usage: 'sbt "run <path_to_file_with_credentials>"' """)
    System.exit(1)
  }

  val saunaConfig = SaunaConfig(new File(args(0)))

  implicit val region = Region.US_WEST_2
  implicit val credentials = new Credentials(saunaConfig.accessKeyId, saunaConfig.secretAccessKey)
  val s3 = S3(credentials)
  val sqs = SQS(credentials)
  val queue = sqs.queue(saunaConfig.queueName)
                 .getOrElse(throw new Exception("No queue with that name found"))

  val s3Observer = new S3Observer(s3, sqs, queue)
  val localObserver = new LocalObserver(saunaConfig.saunaRoot)
  val watchers = Seq(s3Observer, localObserver)

  def process(is: InputStream): Unit =
    fromInputStream(is).getLines()
                       .toSeq
                       .collect(TargetingList)
                       .groupBy(t => (t.projectId, t.listName)) // https://github.com/snowplow/sauna/wiki/Optimizely-responder-user-guide#215-troubleshooting
                       .foreach { case (_, tls) => OptimizelyApi.targetingLists(tls) }

  watchers.foreach(_.watch(process))
}
