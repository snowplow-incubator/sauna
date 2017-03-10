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
import play.api.libs.json.{JsPath, Json, Reads, Writes}
import play.api.libs.ws.WSResponse

// sauna
import utils.wsClient

/**
 * An API wrapper that encapsulates all communication with Slack webhooks.
 *
 * @param webhookUrl Incoming webhook URL - equivalent to an API token.
 * @param logger     A logger actor.
 */
class Slack(webhookUrl: String, logger: ActorRef) {

  import Slack._

  /**
   * Sends a message to an incoming webhook.
   *
   * @param message The body of the message.
   * @return Future WSResponse.
   */
  def sendMessage(message: WebhookMessage): Future[WSResponse] = {
    wsClient.url(webhookUrl)
      .withHeaders("Content-Type" -> "application/json")
      .post(Json.toJson(message))
  }
}

object Slack {
  /**
   * Represents a Slack message that can be accepted by an incoming webhook. All
   * elements are optional - some additional validation is required, but delegated to
   * JSON schema validation.
   *
   * @see [[https://api.slack.com/docs/messages Slack API reference]]
   */
  case class WebhookMessage(
    text: Option[String],
    username: Option[String],
    channel: Option[String],
    iconUrl: Option[String],
    iconEmoji: Option[String],
    linkNames: Option[Boolean],
    mrkdwn: Option[Boolean],
    unfurlMedia: Option[Boolean],
    unfurlLinks: Option[Boolean],
    attachments: Option[Vector[WebhookAttachment]]
  )

  /**
   * Represents a Slack message attachment.
   *
   * @see [[https://api.slack.com/docs/message-attachments Slack API reference]]
   */
  case class WebhookAttachment(
    fallback: String,
    color: Option[String],
    pretext: Option[String],
    authorName: Option[String],
    authorLink: Option[String],
    title: Option[String],
    titleLink: Option[String],
    text: Option[String],
    fields: Option[Vector[WebhookAttachmentField]],
    imageUrl: Option[String],
    thumbUrl: Option[String],
    footer: Option[String],
    footerIcon: Option[String],
    ts: Option[Int]
  )

  /**
   * Represents a Slack attachment's field - a short key/value pair
   * that will be displayed as a table cell in the attachment.
   *
   * @see [[https://api.slack.com/docs/message-attachments#fields Slack API reference]]
   */
  case class WebhookAttachmentField(
    title: Option[String],
    value: Option[String],
    short: Option[Boolean]
  )

  implicit val webhookAttachmentFieldReads: Reads[WebhookAttachmentField] = (
    (JsPath \ "title").readNullable[String] and
      (JsPath \ "value").readNullable[String] and
      (JsPath \ "short").readNullable[Boolean]
    ) (WebhookAttachmentField.apply _)

  implicit val webhookAttachmentReads: Reads[WebhookAttachment] = (
    (JsPath \ "fallback").read[String] and
      (JsPath \ "color").readNullable[String] and
      (JsPath \ "pretext").readNullable[String] and
      (JsPath \ "author_name").readNullable[String] and
      (JsPath \ "author_link").readNullable[String] and
      (JsPath \ "title").readNullable[String] and
      (JsPath \ "title_link").readNullable[String] and
      (JsPath \ "text").readNullable[String] and
      (JsPath \ "fields").readNullable[Vector[WebhookAttachmentField]] and
      (JsPath \ "image_url").readNullable[String] and
      (JsPath \ "thumb_url").readNullable[String] and
      (JsPath \ "footer").readNullable[String] and
      (JsPath \ "footer_icon").readNullable[String] and
      (JsPath \ "ts").readNullable[Int]
    ) (WebhookAttachment.apply _)

  implicit val webhookMessageReads: Reads[WebhookMessage] = (
    (JsPath \ "text").readNullable[String] and
      (JsPath \ "username").readNullable[String] and
      (JsPath \ "channel").readNullable[String] and
      (JsPath \ "icon_url").readNullable[String] and
      (JsPath \ "icon_emoji").readNullable[String] and
      (JsPath \ "link_names").readNullable[Boolean] and
      (JsPath \ "mrkdwn").readNullable[Boolean] and
      (JsPath \ "unfurl_media").readNullable[Boolean] and
      (JsPath \ "unfurl_links").readNullable[Boolean] and
      (JsPath \ "attachments").readNullable[Vector[WebhookAttachment]]
    ) (WebhookMessage.apply _)

  implicit val webhookAttachmentFieldWrites: Writes[WebhookAttachmentField] = (
    (JsPath \ "title").writeNullable[String] and
      (JsPath \ "value").writeNullable[String] and
      (JsPath \ "short").writeNullable[Boolean]
    ) (unlift(WebhookAttachmentField.unapply))

  implicit val webhookAttachmentWrites: Writes[WebhookAttachment] = (
    (JsPath \ "fallback").write[String] and
      (JsPath \ "color").writeNullable[String] and
      (JsPath \ "pretext").writeNullable[String] and
      (JsPath \ "author_name").writeNullable[String] and
      (JsPath \ "author_link").writeNullable[String] and
      (JsPath \ "title").writeNullable[String] and
      (JsPath \ "title_link").writeNullable[String] and
      (JsPath \ "text").writeNullable[String] and
      (JsPath \ "fields").writeNullable[Vector[WebhookAttachmentField]] and
      (JsPath \ "image_url").writeNullable[String] and
      (JsPath \ "thumb_url").writeNullable[String] and
      (JsPath \ "footer").writeNullable[String] and
      (JsPath \ "footer_icon").writeNullable[String] and
      (JsPath \ "ts").writeNullable[Int]
    ) (unlift(WebhookAttachment.unapply))

  implicit val webhookMessageWrites: Writes[WebhookMessage] = (
    (JsPath \ "text").writeNullable[String] and
      (JsPath \ "username").writeNullable[String] and
      (JsPath \ "channel").writeNullable[String] and
      (JsPath \ "icon_url").writeNullable[String] and
      (JsPath \ "icon_emoji").writeNullable[String] and
      (JsPath \ "link_names").writeNullable[Boolean] and
      (JsPath \ "mrkdwn").writeNullable[Boolean] and
      (JsPath \ "unfurl_media").writeNullable[Boolean] and
      (JsPath \ "unfurl_links").writeNullable[Boolean] and
      (JsPath \ "attachments").writeNullable[Vector[WebhookAttachment]]
    ) (unlift(WebhookMessage.unapply))
}
