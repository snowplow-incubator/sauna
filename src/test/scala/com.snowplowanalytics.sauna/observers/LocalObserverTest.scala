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
package observers

// java
import java.io.{File, PrintWriter}
import java.nio.file.{Paths, Files}
import java.util.UUID

// scala
import scala.io.Source.fromInputStream

// scalatest
import org.scalatest._

// akka
import akka.actor.ActorSystem
import akka.testkit.TestActorRef

// sauna
import apis.Optimizely
import loggers._
import processors._
import processors.Processor.FileAppeared
import processors.TargetingList.Data


class LocalObserverTest extends FunSuite with BeforeAndAfter {
  implicit var system: ActorSystem = _
  implicit var logger: TestActorRef[Logger] = _

  before {
    system = ActorSystem.create()
    logger = TestActorRef(new StdoutLogger)
  }

  test("integration local") {
    val saunaRoot = "/opt/sauna/"
    val fileName = UUID.randomUUID().toString
    val testFile = new File(saunaRoot + fileName)
    val line1 = "aaaaaa"
    val line2 = "bbbbbb"
    var expectedLines: Seq[String] = null
    val processors = Seq(
      TestActorRef(new Processor {
        override def process(fileAppeared: FileAppeared): Unit = expectedLines = fromInputStream(fileAppeared.is).getLines().toSeq
      })
    )

    val lo = new LocalObserver(saunaRoot, processors)
    new Thread(lo).start()

    Thread.sleep(300)

    new PrintWriter(testFile) {
      write(s"$line1\n$line2")

      close()
    }

    Thread.sleep(300)

    assert(!testFile.exists())
    assert(expectedLines === Seq(line1, line2))
  }
}