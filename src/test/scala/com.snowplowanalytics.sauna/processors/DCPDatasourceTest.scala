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
package processors

// scala
import scala.io.Source.fromFile

// scalatest
import java.io.ByteArrayInputStream

import org.scalatest._

// akka
import akka.actor.ActorSystem
import akka.testkit.TestActorRef

// sauna
import apis.{Optimizely, DummyOptimizely}
import loggers.{LoggerActorWrapper, MutedLogger}

class DCPDatasourceTest extends FunSuite with BeforeAndAfter  {
  implicit var system: ActorSystem = _
  implicit var logger: LoggerActorWrapper = _
  var dummyOptimizely: Optimizely = _

  before {
    system = ActorSystem.create()
    logger = new LoggerActorWrapper(TestActorRef(new MutedLogger))
    dummyOptimizely = new DummyOptimizely
  }

  test("correct line") {
    val line1 = """"t"    "123"    "alpha"    "2013-12-15 14:05:06.789""""
    val line2 = """"f"    "456"    "delta"    "2014-06-10 21:48:32.712""""
    val line3 = """"f"    "789"    "omega"    "2015-01-28 07:32:16.329""""
    val dcpDatasource = TestActorRef(new DCPDatasource(dummyOptimizely, "", "")).underlyingActor

    assert(dcpDatasource.correct(line1) == "true,123,alpha,1387116306789")
    assert(dcpDatasource.correct(line2) == "false,456,delta,1402436912712")
    assert(dcpDatasource.correct(line3) == "false,789,omega,1422430336329")
  }

  test("correct line with invalid date") {
    val line = """"falsetrue"    "123"    "alpha"    "2013qqqqq12-15 14:05:06.789""""
    val dcpDatasource = TestActorRef(new DCPDatasource(dummyOptimizely, "", "")).underlyingActor

    assert(dcpDatasource.correct(line) == "falsetrue,123,alpha,2013qqqqq12-15 14:05:06.789")
  }

  test("correct line with wrong date") { // let this be a feature
    val line = """"t"    "123"    "alpha"    "2013-99-15 14:05:06.789""""
    val dcpDatasource = TestActorRef(new DCPDatasource(dummyOptimizely, "", "")).underlyingActor

    assert(dcpDatasource.correct(line) == "true,123,alpha,1615817106789")
  }

  test("correct file") {
    val data = """"t"    "123"    "alpha"    "2013-12-15 14:05:06.789"
                 |"false"    "456"    "delta"    "2014-06-10 21:48:32.712"
                 |"f"    "789"    "omega"    "2015-01-28 07:32:16.329"""".stripMargin
    val inputStream = new ByteArrayInputStream(data.getBytes("UTF-8"))
    val header = "isVip,customerId,spendSegment,whenCreated"
    val dcpDatasource = TestActorRef(new DCPDatasource(dummyOptimizely, "", "")).underlyingActor
    val file = dcpDatasource.correct(inputStream, header)

    assert(fromFile(file).getLines()
                         .mkString("\n") == """isVip,customerId,spendSegment,whenCreated
                                              |true,123,alpha,1387116306789
                                              |false,456,delta,1402436912712
                                              |false,789,omega,1422430336329""".stripMargin)
    val _ = file.delete() // it's in /tmp
  }
}