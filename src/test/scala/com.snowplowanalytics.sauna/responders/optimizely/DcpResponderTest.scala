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
package responders
package optimizely

// java
import java.io.ByteArrayInputStream
import java.nio.file.Paths

// scala
import scala.io.Source.fromFile

// scalatest
import org.scalatest._

// sauna
import observers.Observer.LocalFilePublished


class DcpResponderTest extends FunSuite {

  test("correct line") {

    val line1 = """"t"	"123"	"alpha"	"2013-12-15 14:05:06.789""""
    val line2 = """"f"	"456"	"delta"	"2014-06-10 21:48:32.712""""
    val line3 = """"f"	"789"	"omega"	"2015-01-28 07:32:16.329""""

    DcpResponder.correctLine(line1).contains("true,123,alpha,1387116306789")
    DcpResponder.correctLine(line2).contains("false,456,delta,1402436912712")
    DcpResponder.correctLine(line3).contains("false,789,omega,1422430336329")
  }

  test("correct line with invalid date") {
    val line = """"falsetrue"	"123"	"alpha"	"2013qqqqq12-15 14:05:06.789""""

    DcpResponder.correctLine(line).contains("falsetrue,123,alpha,2013qqqqq12-15 14:05:06.789")
  }

  test("correct line with wrong date") {
    val line = """"t"	"123"	"alpha"	"2013-99-15 14:05:06.789""""

    DcpResponder.correctLine(line).isEmpty
  }

  test("correct file") {
    val data = """"t"	"123"	"alpha"	"2013-12-15 14:05:06.789"
                 |"false"	"456"	"delta"	"2014-06-10 21:48:32.712"
                 |"f"	"789"	"omega"	"2015-01-28 07:32:16.329"""".stripMargin
    val inputStream = new ByteArrayInputStream(data.getBytes("UTF-8"))
    val header = "isVip,customerId,spendSegment,whenCreated"
    val file = DcpResponder.correct(inputStream, header)

    assert(fromFile(file).getLines()
                         .mkString("\n") == """isVip,customerId,spendSegment,whenCreated
                                              |true,123,alpha,1387116306789
                                              |false,456,delta,1402436912712
                                              |false,789,omega,1422430336329""".stripMargin)
    val _ = file.delete() // it's in /tmp
  }

  test("correct filename") {
    val fileName = "/home/joe/com.optimizely.dcp/datasource/v1/123456790/987665421/tsv:isVip,customerId,spendSegment,birth/ua-team/joe.tsv"
    val responderEvent = DcpResponder.extractCustomerPublished(LocalFilePublished(Paths.get(fileName), null))
    responderEvent match {
      case Right(e) =>
        val destName = DcpResponder.correctName(e)
        assume(destName == "ua-team/joe.csv")
      case _ => fail("Cannot extract responder event from ObserverBatchEvent observer event")
    }
  }
}
