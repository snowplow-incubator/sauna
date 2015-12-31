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
package apis

// scala
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

// akka
import akka.actor.ActorRef

// sauna
import processors.TargetingList.Data

class DummyOptimizely(implicit logger: ActorRef) extends Optimizely {
  override def postTargetingLists(tlData: Seq[Data], token: String): Unit = {}

  override def getOptimizelyS3Credentials(dcpDatasourceId: String, token: String): Future[Option[(String, String)]] = Future(None)
}