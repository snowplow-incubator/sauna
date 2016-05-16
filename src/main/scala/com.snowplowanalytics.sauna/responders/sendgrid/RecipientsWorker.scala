/*
 * Copyright (c) 2012-2016 Snowplow Analytics Ltd. All rights reserved.
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

// akka
import akka.actor._

// nscala-time
import com.github.nscala_time.time.StaticDateTimeFormat

// scala
import scala.concurrent.duration._
import scala.collection.mutable
import scala.util.control.NonFatal
import scala.util.{ Try, Success, Failure }

// play json
import play.api.libs.json._

// sauna
import apis.Sendgrid
import loggers.Logger.Notification
import RecipientsResponder._
import Responder._


/**
 * Worker actor responsible for posting recipients lists to Sengrid endpoint
 * with predefined interval (3 times in 2 seconds)
 *
 * @param apiWrapper Sendgrid API Wrapper
 */
class RecipientsWorker(apiWrapper: Sendgrid) extends Actor {
  import RecipientsWorker._

  import context.dispatcher

  /**
   * Queue of iterators of chunks awaiting to be processed
   */
  val chunksQueue: mutable.Queue[RecipientsChunks] = mutable.Queue.empty

  /**
   * Currently processing iterator of TSV lines
   */
  var currentChunks: Option[RecipientsChunks] = None

  def receive = {
    case Delegate(chunks) =>
      if (currentChunks.isEmpty) {
        currentChunks = Some(chunks)
        context.system.scheduler.scheduleOnce(WAIT_TIME.milliseconds, self, Tick)
      } else {
        chunksQueue.enqueue(chunks)
      }

    case Tick =>
      currentChunks match {
        case Some(chunks) if chunks.chunkIterator.hasNext =>
          processData(chunks.source.attrs, chunks.chunkIterator.next())
        case Some(empty) =>
          currentChunks = None
          context.parent ! RecipientsProcessed(empty.source, s"Recipients from [${empty.source.source.path}] have been processed")
        case None if chunksQueue.nonEmpty =>
          currentChunks = Some(chunksQueue.dequeue())
        case None => ()
      }

      context.system.scheduler.scheduleOnce(WAIT_TIME.milliseconds, self, Tick)
  }

  /**
   * It handles errors and sends result to Sendgrid.
   *
   * @param keys Seq of attribute keys, repeated for each recipient from `chunk`.
   * @param chunk Seq of recipients, where recipient is a seq of attribute values.
   *                Each `values` in `chunk` should have one length with `keys`.
   */
  def processData(keys: List[String], chunk: Lines[TSV]): Unit = {
    val json = makeValidJson(keys, chunk)
    apiWrapper
      .postRecipients(json)
      .onComplete {
        case Success(response) => processResponse(chunk.length, response.body)
        case Failure(err) => notify(err.toString)
      }
  }

  /**
   * Used to notify about possible errors from Sendgrid's response.
   * Informs user via logger ! Notification.
   * Heavily relies to Sendgrid's response json structure.
   *
   * @param totalRecordsNumber Sometimes records "disappear".
   *                           So, originally were 10, error_count = 2, updated_count = 4, new_count = 3.
   *                           One record was lost. This param is expected total records number.
   * @param jsonText A json text from Sendgrid. Example:
   * @see https://sendgrid.com/docs/API_Reference/Web_API_v3/Marketing_Campaigns/contactdb.html#Add-Single-Recipient-POST
   */
  def processResponse(totalRecordsNumber: => Int, jsonText: String): Unit = {
    lazy val total = totalRecordsNumber

    try {
      val json = Json.parse(jsonText)
      val errorCount = (json \ "error_count").as[Int]
      val errorIndices = (json \ "error_indices").as[Seq[Int]]
      val errorsOpt = (json \ "errors").asOpt[Seq[JsObject]]
      val newCount = (json \ "new_count").as[Int]
      val updatedCount = (json \ "updated_count").as[Int]

      // trying to get error explanation
      for {
        errorIndex <- errorIndices
        errors     <- errorsOpt
      } {
        errors.map(_.value).find(_.apply("error_indices").as[Seq[Int]].contains(errorIndex)) match {
          case Some(error) =>
            val reason = error.apply("message").as[String]
            notify(s"Error $errorIndex caused due to [$reason]")

          case None =>
            notify(s"Unable to find reason for error [$errorIndex]")
        }
      }

      if (errorCount + newCount + updatedCount != total) {
        notify("Several records disappeared. It's rare Sendgrid bug. Double-check you input")
      }

    } catch {
      case NonFatal(e) =>
        notify(s"Got exception [${e.getMessage}] while parsing Sendgrid's response")
    }
  }

  /**
   * Helper method to send notifications
   */
  def notify(message: String): Unit = {
    context.parent ! Notification(message)
  }
}


object RecipientsWorker {
  /**
   * Delay in milliseconds in which Sendgrid can accept next recipients batch
   *
   * @see https://sendgrid.com/docs/API_Reference/Web_API_v3/Marketing_Campaigns/contactdb.html#Add-Recipients-POST
   */
  val WAIT_TIME = 667L

  val dateFormatFull = StaticDateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").withZoneUTC()
  val dateRegexpFull = "^(\\d{1,4}-\\d{1,2}-\\d{1,2} \\d{1,2}:\\d{1,2}:\\d{1,2}\\.\\d{1,3})$".r
  val dateFormatShort = StaticDateTimeFormat.forPattern("yyyy-MM-dd").withZoneUTC()
  val dateRegexpShort = "^(\\d{1,4}-\\d{1,2}-\\d{1,2})$".r

  /**
   * Delegate new piece of work
   *
   * @param chunks object with underlying iterator of TSV lines
   */
  case class Delegate(chunks: RecipientsChunks)

  /**
   * Self-awakening message used to awake worker after `WAIT_TIME`
   */
  case object Tick

  /**
   * Message denoting that recipients file processed successfully
   *
   * @param source responder event
   * @param message string message to log
   */
  case class RecipientsProcessed(source: RecipientsPublished, message: String) extends ResponderResult

  def props(apiWrapper: Sendgrid): Props =
    Props(new RecipientsWorker(apiWrapper))

  /**
   * Transform JSON derived from TSV using following rules:
   * + empty string becomes null
   * + numeric string becomes number
   *
   * @param json one-level JSON object extracted from TSV
   * @return cleaned JSON
   */
  def postProcess(json: JsObject): JsObject = {
    JsObject(json.value.mapValues {
      case JsString("") => JsNull
      case JsString(v)  =>
        Try(v.toDouble).toOption.map(d => JsNumber(d)).getOrElse(JsString(v))
      case other => other
    })
  }

  /**
   * Transform string, possible containing ISO-8601 datetime to string with
   * Unix-epoch (in seconds) or do nothing if string doesn't conform format
   *
   * @param s string to be corrected.
   * @return Corrected word.
   * @see https://github.com/snowplow/sauna/wiki/SendGrid-responder-user-guide#214-response-algorithm
   */
  def correctTimestamps(s: String): String = s match {
    case dateRegexpFull(timestamp) =>
      (dateFormatFull.parseDateTime(timestamp).getMillis / 1000).toString
    case dateRegexpShort(timestamp) =>
      (dateFormatShort.parseDateTime(timestamp).getMillis / 1000).toString
    case _ => s
  }

  /**
   * Creates a Sendgrid-friendly JSON from given keys and values
   * Also transforms everything look-alike datetime (ISO-8601) into Unix epoch
   * Length of `keys` **MUST** be equal to amount of length of inner Seq
   *
   * For example, for `keys` = Seq("name1", "name2"),
   *                  `values` = Seq(Seq("value11", "value12"), Seq("value21", "value22")),
   * result would be:
   *
   * [
   *   {
   *     "name1": "value11",
   *     "name2": "value12"
   *   },
   *   {
   *     "name1": "value21",
   *     "name2": "value22"
   *   }
   * ]
   *
   * @param keys Seq of attribute keys, repeated for each recipient from `chunk`
   * @param values Seq of recipients, where recipient is a seq of attribute values.
   *                Each `values` in `chunk` must already have one length with `keys`
   * @return Sendgrid-friendly JSON
   * @see https://github.com/snowplow/sauna/wiki/SendGrid-responder-user-guide#214-response-algorithm
   */
  private[sendgrid] def makeValidJson(keys: Seq[String], values: Seq[Seq[String]]): JsArray = {
    val recipients = for {
      vals            <- values
      correctedValues  = vals.map(correctTimestamps)}
      yield {
        val zipped = keys.zip(correctedValues.map(x => Json.toJson(x)))
        val recipientData = JsObject(zipped.toMap)
        postProcess(recipientData)
      }

    JsArray(recipients)
  }
}
