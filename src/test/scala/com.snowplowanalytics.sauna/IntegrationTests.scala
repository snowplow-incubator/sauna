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
import java.io.{File, PrintWriter}
import java.nio.file.{Files, Paths}
import java.util.UUID

// scala
import scala.io.Source.fromInputStream

// scalatest
import org.scalatest._

// akka
import akka.actor.{Actor, ActorSystem}
import akka.testkit.TestActorRef

// sauna
import apis.Optimizely
import loggers.Logger.{Manifestation, Notification}
import loggers._
import observers._
import processors._
import processors.Processor._

class IntegrationTests extends FunSuite with BeforeAndAfter {
  implicit var system: ActorSystem = _
  implicit var logger: TestActorRef[Actor] = _

  before {
    system = ActorSystem.create()
    logger = TestActorRef(new MutedLogger)
  }

  after {
    val _ = system.terminate()
  }

  test("local no processor") {
    // prepare for start, define some variables
    val saunaRoot = "/opt/sauna/"
    val fileName = UUID.randomUUID().toString
    val testFile = new File(saunaRoot + fileName)
    val line1 = "aaaaaa"
    val line2 = "bbbbbb"
    var expectedLines: Seq[String] = null
    val processors = Seq(
      TestActorRef(new Processor {
        override def processed(fileAppeared: FileAppeared): Boolean = {
          expectedLines = fromInputStream(fileAppeared.is).getLines().toSeq
          true
        }
      })
    )
    val lo = new LocalObserver(saunaRoot, processors)

    // start observer
    new Thread(lo).start()

    // wait
    Thread.sleep(300)

    // do an action that should trigger observer
    new PrintWriter(testFile) {
      write(s"$line1\n$line2")

      close()
    }

    // wait
    Thread.sleep(300)

    // make sure everything went as expected
    assert(!testFile.exists())
    assert(expectedLines === Seq(line1, line2))
  }

  test("local targeting lists") {
    // prepare for start, define some variables
    val saunaRoot = "/opt/sauna/"
    val source = Paths.get("src/test/resources/targeting_list.tsv")
    val destination = Paths.get("/opt/sauna/com.optimizely/targeting_lists/v1/tsv:*/marketing-team/mary/new-lists.tsv")

    // cleanup
    Files.deleteIfExists(destination)

    // get a secret token
    val secretToken = System.getenv("OPTIMIZELY_PROJECT_TOKEN")
    assert(secretToken != null)

    // this should be changed if TargetingList works properly
    var id: String = null

    // actors (if executed in another thread) silences error
    // approach with testing 'receive' is also impossible,
    // because this test should go as close to real one as possible
    // so, let's introduce a variable that will be assigned if something goes wrong
    var wasError = false

    // define mocked logger
    logger = TestActorRef(new Actor {
      def step1: Receive = {
        case message: Notification =>
          if (!message.text.contains("Detected new local file")) wasError = true
          context.become(step2)

        case _ =>
          wasError = true
      }

      def step2: Receive = {
        case message: Notification =>
          if (message.text != "Successfully uploaded targeting lists with name [dec_ab_group].") wasError = true

        case message: Manifestation =>
          id = message.uid

        case _ =>
          wasError = true
      }

      override def receive = step1
    })

    // define mocked Optimizely
    val optimizely = new Optimizely(secretToken)

    // define real processor and observer
    val processorActors = Seq(TargetingList(optimizely))
    val observers = Seq(new LocalObserver(saunaRoot, processorActors))
    observers.foreach(new Thread(_).start())

    // wait
    Thread.sleep(500)

    // do an action that should trigger observer
    Files.copy(source, destination)

    // wait, assuming 5 seconds is enough to get to Optimizely and back
    Thread.sleep(5000)

    // cleanup
    optimizely.deleteTargetingList(id)

    // id should be updated
    assert(id != null)

    // make sure everything went as expected
    assert(!wasError)
  }
}