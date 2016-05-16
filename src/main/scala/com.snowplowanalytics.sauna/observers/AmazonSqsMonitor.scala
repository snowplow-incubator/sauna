/*
 * Copyright (c) 2012-2016 Snowplow Analytics Ltd. All rights reserved.
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

// awscala
import awscala.sqs.{Message, Queue, SQS}

/**
 * Observes some AWS S3 bucket
 *
 * @param sqs both with `queue` provide actions for AWS SQS
 * @param queue both with `sqs` provide actions for AWS SQS
 * @param onMessage callback reacting on new message
 * @param onException callback reacting on exception thrown by Monitor
 */
class AmazonSqsMonitor(
    sqs: SQS,
    queue: Queue,
    onMessage: Message => Unit,
    onException: Throwable => Unit
  ) extends Runnable {

  @volatile private var running = true

  override def run(): Unit = {
    try {
      while (running) {
        sqs.receiveMessage(queue, count = 1, wait = 5).foreach(onMessage)
      }
    } catch {
      case NonFatal(e) => onException(e)
    }
  }

  def stop(): Unit = {
    running = false
  }
}
