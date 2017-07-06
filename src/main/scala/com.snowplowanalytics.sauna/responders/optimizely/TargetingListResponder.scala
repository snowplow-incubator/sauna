/*
 * Copyright (c) 2016-2017 Snowplow Analytics Ltd. All rights reserved.
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
import java.io.StringReader
import java.util.UUID

// scala
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.Source.fromInputStream
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

// akka
import akka.actor.{ActorRef, Props}

// play
import play.api.libs.json.Json
import play.api.libs.ws.WSResponse

// jackson
import com.fasterxml.jackson.core.JsonParseException

// scala-csv
import com.github.tototoshi.csv._

// sauna
import apis.Optimizely
import loggers.Logger.Manifestation
import observers.Observer._
import responders.Responder._
import responders.optimizely.TargetingListResponder._
import utils._

/**
 * Does stuff for Optimizely Targeting List feature.
 *
 * @see https://github.com/snowplow/sauna/wiki/Optimizely-responder-user-guide#targeting-list
 * @param optimizely Instance of Optimizely.
 * @param logger     A logger actor.
 */
class TargetingListResponder(optimizely: Optimizely, val logger: ActorRef) extends Responder[ObserverFileEvent, TargetingListPublished] {

  def extractEvent(observerEvent: ObserverEvent): Option[TargetingListPublished] = {
    observerEvent match {
      case e: ObserverFileEvent =>
        if (e.id.matches(pathPattern)) Some(TargetingListPublished(e))
        else None
      case _ => None
    }
  }

  /**
   * Extract data from tsv, group by project id and post to endpoint
   */
  def process(event: TargetingListPublished): Unit = {
    event.source.streamContent match {
      case Some(content) =>
        val iterables = fromInputStream(content)
          .getLines.toList.flatMap(extract) // Create TargetingListResponder.TargetingListLine from each line
          .groupBy(t => (t.projectId, t.listName)) // https://github.com/snowplow/sauna/wiki/Optimizely-responder-user-guide#215-troubleshooting
          .map { case (_, tls) => optimizely.postTargetingLists(tls) } // For each group make an *async* upload; TODO: limit may be required

        Future.sequence(iterables).map(_.foreach(logResponse)).onComplete {
          case Success(_) =>
            context.parent ! TargetingListUploaded(event, s"Targeting list from ${event.source.id} has been successfully published")
          case Failure(error) => notifyLogger(error.toString)
        }
      case None => notifyLogger(s"Cannot read file [${event.source.id}]")
    }
  }

  /**
   * Parse Optimizely response and notify logger about result
   *
   * @param response Optimizely API response containing JSON
   */
  def logResponse(response: WSResponse): Unit = {
    lazy val defaultId = UUID.randomUUID.toString
    lazy val defaultDescription = response.body
    lazy val defaultLastModified = new java.sql.Timestamp(System.currentTimeMillis).toString
    val defaultName = "Not found"
    val status = response.status

    try {
      // response.body is valid json
      val json = Json.parse(response.body)
      val id = (json \ "id").asOpt[Long].orElse((json \ "uuid").asOpt[String]).getOrElse(defaultId)
      val name = (json \ "name").asOpt[String].getOrElse(defaultName)
      val description = (json \ "description").asOpt[String]
        .orElse((json \ "message").asOpt[String])
        .getOrElse(defaultDescription)
      val lastModified = (json \ "last_modified").asOpt[String]
        .getOrElse(defaultLastModified)

      // log results
      logger ! Manifestation(id.toString, name, status, description, lastModified)
      if (status == 201) {
        notifyLogger(s"Successfully uploaded targeting lists with name [$name]")
      } else {
        notifyLogger(s"Unable to upload targeting list with name [$name] : [${response.body}]")
      }

    } catch {
      case e: JsonParseException =>
        logger ! Manifestation(defaultId, defaultName, status, defaultDescription, defaultLastModified)
        notifyLogger(s"Problems while parsing Optimizely API response. See [${response.body}]")
    }
  }
}

object TargetingListResponder {

  /**
   * Event denoting that some observer found file that contain targeting lists
   *
   * @param source original observer event
   */
  case class TargetingListPublished(
    source: ObserverFileEvent
  ) extends ResponderEvent

  /**
   * Event denoting that targeting list has been successfully processed and uploaded
   *
   * @param source  original responder event
   * @param message success message
   */
  case class TargetingListUploaded(
    source: TargetingListPublished,
    message: String
  ) extends ResponderResult

  val pathPattern =
    """.*com\.optimizely/
      |targeting_lists/
      |v1/
      |tsv:\*/
      |.+$
    """.stripMargin
      .replaceAll("[\n ]", "")

  /**
   * Constructs a Props for TargetingListResponder actor.
   *
   * @param optimizely Instance of Optimizely
   * @param logger     Actor with underlying Logger
   * @return Props for new actor.
   */
  def props(optimizely: Optimizely, logger: ActorRef): Props =
    Props(new TargetingListResponder(optimizely, logger))

  /**
   * Extract TargetingListLine from given string
   *
   * @param line A string to be extracting from.
   * @return data if line conforms to expected format
   */
  def extract(line: String): Option[Optimizely.TargetingListLine] = {
    val reader = CSVReader.open(new StringReader(line))(tsvFormat)

    try {
      reader.readNext() match {
        case Some(List(projectId: String, listName: String, listDescription: String, listType: String, keyFieldsOrig: String, value: String)) =>
          val keyFields = if (keyFieldsOrig.isEmpty) None else Some(keyFieldsOrig)
          Some(Optimizely.TargetingListLine(projectId, listName, listDescription, listType.toShort, keyFields, value))
        case _ => None
      }
    } catch {
      case NonFatal(_) => None
    } finally {
      reader.close()
    }
  }
}