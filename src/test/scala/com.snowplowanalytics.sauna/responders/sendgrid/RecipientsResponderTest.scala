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
import akka.actor.{Actor, Props, ActorSystem, ActorRef}
import akka.pattern.ask
import akka.util.Timeout

// play
import play.api.libs.json._
import play.api.libs.ws.WSResponse
import play.api.libs.ws.ning.NingWSResponse

// sauna
import actors.Mediator
import apis.Sendgrid
import observers.Observer.LocalFilePublished
import Responder.ResponderResult
import RecipientsResponder.RecipientsPublished


object RecipientsResponderTest {
  object MockResponse extends NingWSResponse(null) {
    override lazy val body = """{}"""
  }

  /**
   * Supervisor actor tracking execution time
   */
  class RootActor(respondersProps: List[Props]) extends Mediator(SaunaSettings(None, None, None, None, Nil, Nil)) {
    override val logger = context.actorOf(Props(new NoopActor))
    override val observers = List(context.actorOf(Props(new NoopActor)))
    override val responderActors = respondersProps.map(p => context.actorOf(p))
    var mocksSent = 0
    var results = 0
    var finished: Long = 0L

    def readyToAnswer: Receive = { case WhenFinished => sender() ! finished }

    def receiveResults: Receive = {
      case result: ResponderResult =>
        results = results + 1
        if (results == 2) {
          finished = System.currentTimeMillis()
          context.become(readyToAnswer)
        }
    }

    override def receive = {
      case mock: MockLocalFilePublished =>
        mocksSent = mocksSent + 1
        responderActors.foreach { _ ! mock }
        if (mocksSent == 2) context.become(receiveResults)
    }
  }

  class NoopActor extends Actor {
    def receive = {
      case _ => ()
    }
  }

  val filePath = "some-non-existing-file-123/opt/sauna/com.sendgrid.contactdb/recipients/v1/tsv:email,birthday,middle_name,favorite_number,when_promoted/ua-team/joe/warehouse.tsv"
  class MockLocalFilePublished(data: String, observer: ActorRef) extends LocalFilePublished(java.nio.file.Paths.get(filePath), observer) {
    override def streamContent = Some(new ByteArrayInputStream(data.getBytes("UTF-8")))
  }

  case object WhenFinished

}

class RecipientsResponderTest extends FunSuite with BeforeAndAfter {
  import RecipientsResponderTest._

  implicit val timeout = Timeout(11.seconds)
  implicit var system: ActorSystem = _
  var noopRef: ActorRef = _

  before {
    system = ActorSystem("RecipientsResponderTest")
    noopRef = system.actorOf(Props(new NoopActor))
  }

  after {
    system.terminate()
  }

  test("makeValidJson valid data") {
    val keys = Seq("email", "birthday", "middle_name", "favorite_number", "when_promoted")
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

    val reifiedIterator = RecipientsResponder.RecipientsChunks.parse(is, RecipientsPublished(keys, new MockLocalFilePublished(valuess, noopRef)), log _).chunkIterator.toList

    assert(logMessage == expectedMessage, "log message should be emitted")
    assert(reifiedIterator.head.size == 2, "end result should consist only from valid lines")
  }

  test("makeValidJson using \"null\" (without quotations) instead of \"\"") {
    val keys = Seq("email", "birthday", "middle_name", "favorite_number", "when_promoted")
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

  test("makeValidJson all positive integers should be without quotations") {
    val keys = Seq("email", "birthday", "middle_name", "favorite_number", "when_promoted")
    val valuess = Seq(
      "\"11111111111\"\t\"1980-06-21\"\t\"Al\"\t\"13\"\t\"2013-12-15 14:05:06.789\"",
      "\"karl@bar.de\"\t\"1975-07-02\"\t\"\"\t\"12\"\t\"2014-06-10 21:48:32.712\"",
      "\"ed@me.co.uk\"\t\"1992-09-12\"\t\"Jo\"\t\"98\"\t\"2015-01-28 07:32:16.329\""
    ).flatMap(RecipientsResponder.valuesFromTsv)
    val resultJson = RecipientsWorker.makeValidJson(keys, valuess)
    val expected = """[{"email":11111111111,"birthday":330393600,"middle_name":"Al","favorite_number":13,"when_promoted":1387116306},{"email":"karl@bar.de","birthday":173491200,"middle_name":null,"favorite_number":12,"when_promoted":1402436912},{"email":"ed@me.co.uk","birthday":716256000,"middle_name":"Jo","favorite_number":98,"when_promoted":1422430336}]"""
    val expectedJson = Json.parse(expected)

    assert(resultJson === expectedJson)
  }

  test("correctTimestamp no timestamp") {
    val s = "qwerty"
    val expected = s

    assert(RecipientsWorker.correctTimestamps(s) === expected)
  }

  test("correctTimestamp short timestamp") {
    val s = "1980-06-21"
    val expected = "330393600"

    assert(RecipientsWorker.correctTimestamps(s) === expected)
  }

  test("correctTimestamp full timestamp") {
    val s = "2013-12-15 14:05:06.789"
    val expected = "1387116306"

    assert(RecipientsWorker.correctTimestamps(s) === expected)
  }

  test("respect Sendgrid limitations: LINE_LIMIT and WAIT_TIME") {
    var respectLineLimit = true
    val data = (for (i <- 1 to 3000) yield s""""bob$i@foo.com"\t"1980-06-21"\t"Al"\t"13"\t"2013-12-15 14:05:06.789"""")
                  .mkString("\n")
    val logger = system.actorOf(Props(new NoopActor))
    val mockedSendgrid = new Sendgrid("") {
      override def postRecipients(json: JsValue): Future[WSResponse] = {
        json.asOpt[JsArray].foreach { array =>
          if (array.value.length > RecipientsResponder.LINE_LIMIT) {
            respectLineLimit = false
          }
        }
        Future.successful(MockResponse)
      }
    }

    val root = system.actorOf(Props(
      new RootActor(List(RecipientsResponder.props(logger, mockedSendgrid))))
    )

    // preparing is done, start timing
    val time = System.currentTimeMillis()

    // simulate two simultaneous messages
    root ! new MockLocalFilePublished(data, noopRef)
    root ! new MockLocalFilePublished(data, noopRef)

    Thread.sleep(8000)

    val end = Await.result((root ? WhenFinished).mapTo[Long], 2.second)

    assert(end - time > 4000, "file was processed too fast") // 2 x 3000 lines == 4 seconds
    assert(respectLineLimit, "too many lines in a single chunk")
  }
}

