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
package sendgrid

// java
import java.io.ByteArrayInputStream

// scalatest
import org.scalatest._

// akka
import akka.actor.{ActorSystem, ActorRef}

// play
import play.api.libs.json._

// sauna
import apis.Sendgrid
import observers.Observer.LocalFilePublished
import RecipientsResponder.RecipientsPublished


object RecipientsResponderTest {
  val filePath = "some-non-existing-file-123/opt/sauna/com.sendgrid.contactdb/recipients/v1/tsv:email,birthday,middle_name,favorite_number,when_promoted/ua-team/joe/warehouse.tsv"
  class MockLocalFilePublished(data: String, observer: ActorRef) extends LocalFilePublished(java.nio.file.Paths.get(filePath), observer) {
    override def streamContent = Some(new ByteArrayInputStream(data.getBytes("UTF-8")))
  }
}

class RecipientsResponderTest extends FunSuite with BeforeAndAfter {
  import RecipientsResponderTest._
  import Sendgrid._

  implicit var system: ActorSystem = _

  before {
    system = ActorSystem("RecipientsResponderTest")
  }

  after {
    system.terminate()
  }

  test("makeValidJson valid data") {
    val keys = List(
      CustomType(1, "email", SendgridText),
      CustomType(2, "birthday", SendgridDate),
      CustomType(3, "middle_name", SendgridText),
      CustomType(4, "favorite_number", SendgridNumber),
      CustomType(5, "when_promoted", SendgridDate)
    )
    val valuess = Seq(
      "\"bob@foo.com\"\t\"1980-06-21\"\t\"Al\"\t\"13\"\t\"2013-12-15 14:05:06.789\"",
      "\"karl@bar.de\"\t\"1975-07-02\"\t\"\"\t\"12\"\t\"2014-06-10 21:48:32.712\"",
      "\"ed@me.co.uk\"\t\"1992-09-12\"\t\"Jo\"\t\"98\"\t\"2015-01-28 07:32:16.329\""
    ).flatMap(RecipientsResponder.valuesFromTsv)
    val resultJson = RecipientsWorker.makeValidJson(keys, valuess)
    val expected = """[{"email":"bob@foo.com","birthday":330393600,"middle_name":"Al","favorite_number":13,"when_promoted":1387116306},{"email":"karl@bar.de","birthday":173491200,"middle_name":null,"favorite_number":12,"when_promoted":1402436912},{"email":"ed@me.co.uk","birthday":716256000,"middle_name":"Jo","favorite_number":98,"when_promoted":1422430336}]"""
    val expectedJson = Json.parse(expected)

    assert(resultJson === expectedJson)
  }

  test("RecipientsChunks parse notifies about invalid lines and skip it") {
    val expectedMessage = "RecipientsResponder: line length unmatch [\"bob@foo.com\"\"1980-06-21\"\t\"Al\"\t\"13\"\t\"2013-12-15 14:05:06.789\"]"
    var logMessage: String = ""
    def log(message: String): Unit = { logMessage = message }

    val keys = List("email", "birthday", "middle_name", "favorite_number", "when_promoted")
    val valuess: String = Seq(
      "\"bob@foo.com\"\"1980-06-21\"\t\"Al\"\t\"13\"\t\"2013-12-15 14:05:06.789\"",
      "\"karl@bar.de\"\t\"1975-07-02\"\t\"\"\t\"12\"\t\"2014-06-10 21:48:32.712\"",
      "\"ed@me.co.uk\"\t\"1992-09-12\"\t\"Jo\"\t\"98\"\t\"2015-01-28 07:32:16.329\""
    ).mkString("\n")

    val is = new ByteArrayInputStream(valuess.getBytes)

    val reifiedIterator = RecipientsResponder.RecipientsChunks.parse(is, RecipientsPublished(keys, new MockLocalFilePublished(valuess, null)), log _).chunkIterator.toList

    assert(logMessage == expectedMessage, "log message should be emitted")
    assert(reifiedIterator.head.size == 2, "end result should consist only from valid lines")
  }

  test("makeValidJson using \"null\" (without quotations) instead of \"\"") {
    val keys = List(
      CustomType(1, "email", SendgridText),
      CustomType(2, "birthday", SendgridDate),
      CustomType(3, "middle_name", SendgridText),
      CustomType(4, "favorite_number", SendgridNumber),
      CustomType(5, "when_promoted", SendgridDate)
    )
    val valuess = Seq(
      "\"bob@foo.com\"\t\"1980-06-21\"\t\"Al\"\t\"13\"\t\"2013-12-15 14:05:06.789\"",
      "\"karl@bar.de\"\t\"1975-07-02\"\t\"null\"\t\"12\"\t\"2014-06-10 21:48:32.712\"",
      "\"ed@me.co.uk\"\t\"1992-09-12\"\t\"Jo\"\t\"98\"\t\"2015-01-28 07:32:16.329\""
    ).flatMap(RecipientsResponder.valuesFromTsv)
    val resultJson = RecipientsWorker.makeValidJson(keys, valuess)
    val expected = """[{"email":"bob@foo.com","birthday":330393600,"middle_name":"Al","favorite_number":13,"when_promoted":1387116306},{"email":"karl@bar.de","birthday":173491200,"middle_name":"null","favorite_number":12,"when_promoted":1402436912},{"email":"ed@me.co.uk","birthday":716256000,"middle_name":"Jo","favorite_number":98,"when_promoted":1422430336}]"""
    val expectedJson = Json.parse(expected)

    assert(resultJson === expectedJson)
  }

  test("makeValidJson integers should be without quotations") {
    val keys = List(
      CustomType(1, "email", SendgridText),
      CustomType(2, "birthday", SendgridDate),
      CustomType(3, "middle_name", SendgridText),
      CustomType(4, "favorite_number", SendgridNumber),
      CustomType(5, "when_promoted", SendgridDate)
    )
    val valuess = Seq(
      "\"11111111111\"\t\"1980-06-21\"\t\"Al\"\t\"13\"\t\"2013-12-15 14:05:06.789\"",
      "\"karl@bar.de\"\t\"1975-07-02\"\t\"\"\t\"12\"\t\"2014-06-10 21:48:32.712\"",
      "\"ed@me.co.uk\"\t\"1992-09-12\"\t\"Jo\"\t\"98\"\t\"2015-01-28 07:32:16.329\""
    ).flatMap(RecipientsResponder.valuesFromTsv)
    val resultJson = RecipientsWorker.makeValidJson(keys, valuess)
    val expected = """[{"email":"11111111111","birthday":330393600,"middle_name":"Al","favorite_number":13,"when_promoted":1387116306},{"email":"karl@bar.de","birthday":173491200,"middle_name":null,"favorite_number":12,"when_promoted":1402436912},{"email":"ed@me.co.uk","birthday":716256000,"middle_name":"Jo","favorite_number":98,"when_promoted":1422430336}]"""
    val expectedJson = Json.parse(expected)

    assert(resultJson === expectedJson)
  }

  test("correctTimestamp no timestamp") {
    val s = "qwerty"
    val expected = JsString(s)

    assert(Sendgrid.correctTimestamps(s) === expected)
  }

  test("correctTimestamp short timestamp") {
    val s = "1980-06-21"
    val expected = JsNumber(330393600)

    assert(Sendgrid.correctTimestamps(s) === expected)
  }

  test("correctTimestamp full timestamp") {
    val s = "2013-12-15 14:05:06.789"
    val expected = JsNumber(1387116306)

    assert(Sendgrid.correctTimestamps(s) === expected)
  }
}

