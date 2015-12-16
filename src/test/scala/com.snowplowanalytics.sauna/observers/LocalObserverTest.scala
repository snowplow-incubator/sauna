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
package com.snowplowanalytics.sauna.observers

import java.io.{InputStream, PrintWriter, File}
import java.util.UUID
import scala.io.Source.fromInputStream

import org.scalatest._

import com.snowplowanalytics.sauna.loggers.MutedLogger

class LocalObserverTest extends FunSuite {
  test("integration") {
    val path = "/opt/sauna/"
    val fileName = UUID.randomUUID().toString
    val file = new File(path + fileName)
    val line1 = "aaaaaa"
    val line2 = "bbbbbb"
    var expectedLines: Seq[String] = null
    def process(is: InputStream): Unit = expectedLines = fromInputStream(is).getLines().toSeq

    (new LocalObserver(path) with MutedLogger).observe(process)

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
