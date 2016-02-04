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

// scala
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

// scalatest
import org.scalatest._

// akka
import akka.actor.{Props, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout

// play
import play.api.libs.json._
import play.api.libs.ws.WSResponse

// sauna
import apis.Sendgrid
import loggers._
import loggers.Logger._
import responders.Responder._

class RecipientsTest extends FunSuite with BeforeAndAfter {
  val filePath = "some-non-existing-file-123/opt/sauna/com.sendgrid.contactdb/recipients/v1/tsv:email,birthday,middle_name,favorite_number,when_promoted/ua-team/joe/warehouse.tsv"
  implicit val timeout = Timeout(10.seconds)
  implicit var system: ActorSystem = _
  var sendgridToken: String = _

  before {
    system = ActorSystem("RecipientsTest")
    sendgridToken = System.getenv("SENDGRID_TOKEN")
  }

  test("makeValidJson valid data") {
    val keys = Seq("email", "birthday", "middle_name", "favorite_number", "when_promoted")
    val valuess = Seq(
      "\"bob@foo.com\"\t\"1980-06-21\"\t\"Al\"\t\"13\"\t\"2013-12-15 14:05:06.789\"",
      "\"karl@bar.de\"\t\"1975-07-02\"\t\"\"\t\"12\"\t\"2014-06-10 21:48:32.712\"",
      "\"ed@me.co.uk\"\t\"1992-09-12\"\t\"Jo\"\t\"98\"\t\"2015-01-28 07:32:16.329\""
    ).flatMap(Recipients.valuesFromTsv)
    val result = Recipients.makeValidJson(keys, valuess)
    val resultJson = Json.parse(result)
    val expected = """[{"email":"bob@foo.com","birthday":330393600,"middle_name":"Al","favorite_number":13,"when_promoted":1387116306},{"email":"karl@bar.de","birthday":173491200,"middle_name":null,"favorite_number":12,"when_promoted":1402436912},{"email":"ed@me.co.uk","birthday":716256000,"middle_name":"Jo","favorite_number":98,"when_promoted":1422430336}]"""
    val expectedJson = Json.parse(expected)

    assert(resultJson === expectedJson)
  }

  test("makeValidJson should assert invalid lines") {
    val keys = Seq("email", "birthday", "middle_name", "favorite_number", "when_promoted")
    val valuess = Seq(
      "\"bob@foo.com\"\"1980-06-21\"\t\"Al\"\t\"13\"\t\"2013-12-15 14:05:06.789\"",
      "\"karl@bar.de\"\t\"1975-07-02\"\t\"\"\t\"12\"\t\"2014-06-10 21:48:32.712\"",
      "\"ed@me.co.uk\"\t\"1992-09-12\"\t\"Jo\"\t\"98\"\t\"2015-01-28 07:32:16.329\""
    ).flatMap(Recipients.valuesFromTsv)

    val _ = intercept[AssertionError] {
      Recipients.makeValidJson(keys, valuess)
    }
  }

  test("makeValidJson using \"null\" (without quotations) instead of \"\"") {
    val keys = Seq("email", "birthday", "middle_name", "favorite_number", "when_promoted")
    val valuess = Seq(
      "\"bob@foo.com\"\t\"1980-06-21\"\t\"Al\"\t\"13\"\t\"2013-12-15 14:05:06.789\"",
      "\"karl@bar.de\"\t\"1975-07-02\"\t\"null\"\t\"12\"\t\"2014-06-10 21:48:32.712\"",
      "\"ed@me.co.uk\"\t\"1992-09-12\"\t\"Jo\"\t\"98\"\t\"2015-01-28 07:32:16.329\""
    ).flatMap(Recipients.valuesFromTsv)
    val result = Recipients.makeValidJson(keys, valuess)
    val resultJson = Json.parse(result)
    val expected = """[{"email":"bob@foo.com","birthday":330393600,"middle_name":"Al","favorite_number":13,"when_promoted":1387116306},{"email":"karl@bar.de","birthday":173491200,"middle_name":"null","favorite_number":12,"when_promoted":1402436912},{"email":"ed@me.co.uk","birthday":716256000,"middle_name":"Jo","favorite_number":98,"when_promoted":1422430336}]"""
    val expectedJson = Json.parse(expected)

    assert(resultJson === expectedJson)
  }

  test("makeValidJson all positive integers should be without quotations") {
    val keys = Seq("email", "birthday", "middle_name", "favorite_number", "when_promoted")
    val valuess = Seq(
      "\"11111111111\"\t\"1980-06-21\"\t\"Al\"\t\"13\"\t\"2013-12-15 14:05:06.789\"",
      "\"karl@bar.de\"\t\"1975-07-02\"\t\"\"\t\"12\"\t\"2014-06-10 21:48:32.712\"",
      "\"ed@me.co.uk\"\t\"1992-09-12\"\t\"Jo\"\t\"98\"\t\"2015-01-28 07:32:16.329\""
    ).flatMap(Recipients.valuesFromTsv)
    val result = Recipients.makeValidJson(keys, valuess)
    val resultJson = Json.parse(result)
    val expected = """[{"email":11111111111,"birthday":330393600,"middle_name":"Al","favorite_number":13,"when_promoted":1387116306},{"email":"karl@bar.de","birthday":173491200,"middle_name":null,"favorite_number":12,"when_promoted":1402436912},{"email":"ed@me.co.uk","birthday":716256000,"middle_name":"Jo","favorite_number":98,"when_promoted":1422430336}]"""
    val expectedJson = Json.parse(expected)

    assert(resultJson === expectedJson)
  }

  test("correctTimestamp no timestamp") {
    val s = "qwerty"
    val expected = s

    assert(Recipients.correctTimestamps(s) === expected)
  }

  test("correctTimestamp short timestamp") {
    val s = "1980-06-21"
    val expected = "330393600"

    assert(Recipients.correctTimestamps(s) === expected)
  }

  test("correctTimestamp full timestamp") {
    val s = "2013-12-15 14:05:06.789"
    val expected = "1387116306"

    assert(Recipients.correctTimestamps(s) === expected)
  }

  test("handleErrors") {
    val data = Seq(
      "\"bob@foo.com1980-06-21\"\t\"Al\"\t\"13\"\t\"a2013-12-15 14:05:06.789\"",
      "\"karl@bar.de\"\t\"1975-07-02\"\t\"\"\t\"12\"\t\"b2014-06-10 21:48:32.712\""
    ).mkString("\n")
    val expected = Seq(
      "Unable to process [bob@foo.com1980-06-21\tAl\t13\ta2013-12-15 14:05:06.789], it has only 4 columns, when 5 are required.",
      "Error 0 caused due to [date type conversion error]."
    )
    // using custom logger to check if all messages really appeared
    var received = Seq.empty[String]
    implicit val logger = system.actorOf(Props(new Logger {
      override def log(message: Notification): Unit = received :+= message.text

      override def log(message: Manifestation): Unit = {}
    }))
    val sendgrid = new Sendgrid(sendgridToken)
    val recipients = Recipients(sendgrid)

    // send a message, get a Future notification that it was processed
    val f = recipients ? FileAppeared(filePath, new ByteArrayInputStream(data.getBytes("UTF-8")))

    // wait until Future is processed
    Await.ready(f, 10.seconds)

    // wait for communication with Sendgrid
    Thread.sleep(2000)
    
    assert(received == expected)
  }

  test("respect Sendgrid limitations: LINE_LIMIT and WAIT_TIME") {
    val data = (for (i <- 1 to 3000) yield s""""bob$i@foo.com"\t"1980-06-21"\t"Al"\t"13"\t"2013-12-15 14:05:06.789"""")
                  .mkString("\n")
    implicit val logger = system.actorOf(Props(new MutedLogger))
    val mockedSendgrid = new Sendgrid("")(logger) {
      override def postRecipients(json: String): Future[WSResponse] = {
        Json.parse(json)
            .asOpt[JsArray]
            .foreach( array => assert(array.value.length <= Recipients.LINE_LIMIT, "too many lines in a single chunk") )

        Future.failed(new Exception)
      }
    }
    val recipients = Recipients(mockedSendgrid)

    // preparing is done, start timing
    val time = System.currentTimeMillis()

    // simulate two simultaneous messages
    val f1 = recipients ? FileAppeared(filePath, new ByteArrayInputStream(data.getBytes("UTF-8")))
    val f2 = recipients ? FileAppeared(filePath, new ByteArrayInputStream(data.getBytes("UTF-8")))
    // and wait for them
    Await.ready(f1, 10.seconds)
    Await.ready(f2, 10.seconds)

    assert(System.currentTimeMillis() - time > 4000, "file was processed too fast") // 2 x 3000 lines == 4 seconds
  }
}