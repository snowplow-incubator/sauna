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
package apis

// scala
import scala.concurrent.Future

// akka
import akka.actor.ActorRef

// play
import play.api.libs.functional.syntax._
import play.api.libs.json._
import play.api.libs.ws.WSResponse

// sauna
import utils.wsClient

/**
 * An API wrapper for the PagerDuty Events API.
 *
 * @param logger A Sauna logger actor.
 */
class PagerDuty(logger: ActorRef) {

  import PagerDuty._

  /**
   * Posts a JSON body of a PagerDuty event to the API.
   *
   * @param event The body of the event.
   * @return Future WSResponse.
   */
  def createEvent(event: PagerDutyEvent): Future[WSResponse] = {
    wsClient.url(url)
      .withHeaders("Content-Type" -> "application/json")
      .post(Json.toJson(event))
  }

}

object PagerDuty {
  val url = "https://events.pagerduty.com/generic/2010-04-15/create_event.json"

  /**
   * Represents a PagerDuty event that either triggers, acknowledges, or resolves
   * an incident in the system.
   *
   * @see [[https://v2.developer.pagerduty.com/docs/trigger-events PagerDuty API reference]]
   */
  case class PagerDutyEvent(
    serviceKey: String,
    eventType: String,
    incidentKey: Option[String],
    description: Option[String],
    details: Option[JsValue],
    client: Option[String],
    clientUrl: Option[String],
    contexts: Option[Vector[PagerDutyEventContext]]
  )

  /**
   * Represents a PagerDuty event context - a short asset that can be attached to an incident.
   *
   * @see [[https://v2.developer.pagerduty.com/docs/trigger-events#contexts PagerDuty API reference]]
   */
  case class PagerDutyEventContext(
    `type`: String,
    src: Option[String],
    text: Option[String],
    href: Option[String],
    alt: Option[String]
  )

  implicit val eventContextReads: Reads[PagerDutyEventContext] = (
    (JsPath \ "type").read[String] and
      (JsPath \ "src").readNullable[String] and
      (JsPath \ "text").readNullable[String] and
      (JsPath \ "href").readNullable[String] and
      (JsPath \ "alt").readNullable[String]
    ) (PagerDutyEventContext.apply _)

  implicit val eventReads: Reads[PagerDutyEvent] = (
    (JsPath \ "service_key").read[String] and
      (JsPath \ "event_type").read[String] and
      (JsPath \ "incident_key").readNullable[String] and
      (JsPath \ "description").readNullable[String] and
      (JsPath \ "details").readNullable[JsValue] and
      (JsPath \ "client").readNullable[String] and
      (JsPath \ "client_url").readNullable[String] and
      (JsPath \ "context").readNullable[Vector[PagerDutyEventContext]]
    ) (PagerDutyEvent.apply _)

  implicit val eventContextWrites: Writes[PagerDutyEventContext] = Json.writes[PagerDutyEventContext]

  implicit val eventWrites: Writes[PagerDutyEvent] = (
    (JsPath \ "service_key").write[String] and
      (JsPath \ "event_type").write[String] and
      (JsPath \ "incident_key").writeNullable[String] and
      (JsPath \ "description").writeNullable[String] and
      (JsPath \ "details").writeNullable[JsValue] and
      (JsPath \ "client").writeNullable[String] and
      (JsPath \ "client_url").writeNullable[String] and
      (JsPath \ "context").writeNullable[Vector[PagerDutyEventContext]]
    ) (unlift(PagerDutyEvent.unapply))
}