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
import java.util.UUID

// scala
import scala.io.Source.fromInputStream

// scalatest
import org.scalatest._

// akka
import akka.actor.ActorSystem
import akka.testkit.TestActorRef

// sauna
import loggers.{LoggerActor, MutedLogger}
import processors._
import processors.Processor.FileAppeared


class LocalObserverTest extends FunSuite with BeforeAndAfter {
  implicit var system: ActorSystem = _

  before {
    system = ActorSystem.create()
  }

  test("integration") {
    val path = "/opt/sauna/"
    val fileName = UUID.randomUUID().toString
    val file = new File(path + fileName)
    val line1 = "aaaaaa"
    val line2 = "bbbbbb"
    var expectedLines: Seq[String] = null

    val processors = Seq(
      new ProcessorActor(TestActorRef(new Processor {
        override def process(fileAppeared: FileAppeared): Unit = expectedLines = fromInputStream(fileAppeared.is).getLines().toSeq
      }))
    )

    val logger = new LoggerActor(TestActorRef(new MutedLogger))
    val lo = new LocalObserver(path, processors)(logger)
    new Thread(lo).start()

    Thread.sleep(100)

    new PrintWriter(file) {
      write(s"$line1\n$line2")

      close()
    }

    Thread.sleep(100)

    assert(!file.exists())
    assert(expectedLines === Seq(line1, line2))
  }
}