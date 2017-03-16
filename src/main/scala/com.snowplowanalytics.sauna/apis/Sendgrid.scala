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
import scala.util.Try

// nscala-time
import com.github.nscala_time.time.StaticDateTimeFormat

// play
import play.api.libs.functional.syntax._
import play.api.libs.json._
import play.api.libs.ws.WSResponse

// sauna
import utils._

/**
 * Sendgrid API wrapper. Encapsulates all communications with Sendgrid
 *
 * @param apiKeyId Sendgrid token
 */
class Sendgrid(apiKeyId: String) {

  import Sendgrid._

  /**
   * Tries to get a recipient by id.
   *
   * @param id An recipient id.
   * @return Future[Response]
   */
  def getRecipient(id: String): Future[WSResponse] =
    wsClient.url(urlPrefix + s"contactdb/recipients/$id")
      .withHeaders("Authorization" -> s"Bearer $apiKeyId")
      .get

  /**
   * Tries to get types of custom fields in Sendgrid contacts DB
   */
  def getTypeInfo: Future[WSResponse] =
    wsClient.url(urlPrefix + "contactdb/custom_fields")
      .withHeaders("Authorization" -> s"Bearer $apiKeyId")
      .get

  /**
   * Tries to upload several recipients. Note that this function is not limited by
   * Sendgrid's limit in 1500 recipients per second, it does what is said to do.
   *
   * @see https://sendgrid.com/docs/API_Reference/Web_API_v3/Marketing_Campaigns/contactdb.html#Add-Single-Recipient-POST
   * @param json JSON supposed to be an array of objects ready to be sent to Sendgrid
   * @return Future[Response]
   */
  def postRecipients(json: JsValue): Future[WSResponse] =
    wsClient.url(urlPrefix + s"contactdb/recipients")
      .withHeaders("Authorization" -> s"Bearer $apiKeyId", "Content-Type" -> "application/json")
      .post(json)

  /**
   * Tries to delete a recipient by id.
   *
   * @param id An recipient id.
   * @return Future[Response]
   */
  def deleteRecipient(id: String): Future[WSResponse] =
    wsClient.url(urlPrefix + s"contactdb/recipients/$id")
      .withHeaders("Authorization" -> s"Bearer $apiKeyId")
      .delete()

  /**
   * Sends an email using the Sendgrid API.
   *
   * @param email An email object.
   * @return Future WSResponse.
   */
  def sendEmail(email: SendgridEmail): Future[WSResponse] =
    wsClient.url(urlPrefix + "mail/send")
      .withHeaders("Authorization" -> s"Bearer $apiKeyId", "Content-Type" -> "application/json")
      .post(Json.toJson(email))
}


object Sendgrid {
  val urlPrefix = "https://api.sendgrid.com/v3/"

  // Possible date formats
  val dateFormatFull = StaticDateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").withZoneUTC()
  val dateRegexpFull = "^(\\d{1,4}-\\d{1,2}-\\d{1,2} \\d{1,2}:\\d{1,2}:\\d{1,2}\\.\\d{1,3})$".r
  val dateFormatShort = StaticDateTimeFormat.forPattern("yyyy-MM-dd").withZoneUTC()
  val dateRegexpShort = "^(\\d{1,4}-\\d{1,2}-\\d{1,2})$".r

  /**
   * Transform string, possible containing ISO-8601 datetime to string with
   * Unix-epoch (in seconds) or do nothing if string doesn't conform format
   *
   * @param s string to be corrected.
   * @return Corrected word.
   * @see https://github.com/snowplow/sauna/wiki/SendGrid-responder-user-guide#214-response-algorithm
   */
  def correctTimestamps(s: String): JsValue = s match {
    case dateRegexpFull(timestamp) =>
      JsNumber(dateFormatFull.parseDateTime(timestamp).getMillis / 1000)
    case dateRegexpShort(timestamp) =>
      JsNumber(dateFormatShort.parseDateTime(timestamp).getMillis / 1000)
    case _ => JsString(s)
  }

  /**
   * Correct string (extracted from TSV) according to its type
   */
  type Correct = String => JsValue

  /**
   * Sum type to express available types of fields in Sendgrid contacts DB
   */
  sealed trait SendgridType extends Serializable {
    def correct: Correct
  }
  case object SendgridText extends SendgridType {
    def correct: Correct = {
      case "" => JsNull
      case any => JsString(any)
    }
  }
  case object SendgridDate extends SendgridType {
    def correct: Correct = {
      case "" => JsNull
      case date: String => correctTimestamps(date)
    }
  }
  case object SendgridNumber extends SendgridType {
    def correct: Correct = {
      case "" => JsNull
      case num: String =>
        Try(num.toDouble).toOption.map(d => JsNumber(d)).getOrElse(JsNull)
    }
  }

  /**
   * Information about single custom field in Sendgrid contacts DB
   */
  case class CustomType(id: Long, name: String, `type`: SendgridType)

  /**
   * Information about all custom fields in Sendgrid contacts DB
   */
  case class CustomTypes(customTypes: List[CustomType], ordered: Boolean = false)

  /*
   * Play JSON `Reads` instances to extract `CustomTypes` from API response
   */

  implicit val sendgridTypeReads: Reads[SendgridType] =
    Reads.apply {
      case JsString("text") => JsSuccess(SendgridText)
      case JsString("date") => JsSuccess(SendgridDate)
      case JsString("number") => JsSuccess(SendgridNumber)
      case unknown => JsError(s"Unknown type $unknown encountered")
    }

  implicit val customTypeReads: Reads[CustomType] = (
    (JsPath \ "id").read[Long] and
      (JsPath \ "name").read[String] and
      (JsPath \ "type").read[SendgridType]
    ) (CustomType.apply _)

  implicit val customTypesReads: Reads[CustomTypes] =
    (JsPath \ "custom_fields").read[List[CustomType]].map(types => CustomTypes(types, ordered = false))

  /**
   * Extract custom fields information from HTTP response
   */
  def extractFieldTypes(json: JsValue): Either[String, CustomTypes] = {
    json.validate[CustomTypes].asEither match {
      case Right(customTypes) => Right(customTypes)
      case Left(errors) => Left("Cannot extract custom fields information: " + errors.toString)
    }
  }

  /**
   * Represents an email recipient - may contain the recipient's name,
   * must contain their email.
   */
  case class SendgridEmailObject(
    email: String,
    name: Option[String]
  )

  implicit val sendgridEmailObjectReads: Reads[SendgridEmailObject] = (
    (JsPath \ "email").read[String] and
      (JsPath \ "name").readNullable[String]
    ) (SendgridEmailObject.apply _)
  implicit val sendgridEmailObjectWrites: Writes[SendgridEmailObject] = Json.writes[SendgridEmailObject]

  /**
   * Represents a message's "envelope" - metadata containing information
   * about who should receive an individual message and how it should be handled.
   */
  case class SendgridPersonalization(
    to: Vector[SendgridEmailObject],
    cc: Option[Vector[SendgridEmailObject]],
    bcc: Option[Vector[SendgridEmailObject]],
    subject: Option[String],
    headers: Option[JsValue],
    substitutions: Option[JsValue],
    customArgs: Option[JsValue],
    sendAt: Option[Int]
  )

  implicit val sendgridPersonalizationReads: Reads[SendgridPersonalization] = (
    (JsPath \ "to").read[Vector[SendgridEmailObject]] and
      (JsPath \ "cc").readNullable[Vector[SendgridEmailObject]] and
      (JsPath \ "bcc").readNullable[Vector[SendgridEmailObject]] and
      (JsPath \ "subject").readNullable[String] and
      (JsPath \ "headers").readNullable[JsValue] and
      (JsPath \ "substitutions").readNullable[JsValue] and
      (JsPath \ "custom_args").readNullable[JsValue] and
      (JsPath \ "send_at").readNullable[Int]
    ) (SendgridPersonalization.apply _)
  implicit val sendgridPersonalizationWrites: Writes[SendgridPersonalization] = (
    (JsPath \ "to").write[Vector[SendgridEmailObject]] and
      (JsPath \ "cc").writeNullable[Vector[SendgridEmailObject]] and
      (JsPath \ "bcc").writeNullable[Vector[SendgridEmailObject]] and
      (JsPath \ "subject").writeNullable[String] and
      (JsPath \ "headers").writeNullable[JsValue] and
      (JsPath \ "substitutions").writeNullable[JsValue] and
      (JsPath \ "custom_args").writeNullable[JsValue] and
      (JsPath \ "send_at").writeNullable[Int]
    ) (unlift(SendgridPersonalization.unapply))

  /**
   * The content of an email.
   */
  case class SendgridContent(
    `type`: String,
    value: String
  )

  implicit val sendgridContentReads: Reads[SendgridContent] = (
    (JsPath \ "type").read[String] and
      (JsPath \ "value").read[String]
    ) (SendgridContent.apply _)
  implicit val sendgridContentWrites: Writes[SendgridContent] = Json.writes[SendgridContent]

  /**
   * The attachment of an email.
   */
  case class SendgridAttachment(
    content: String,
    `type`: Option[String],
    filename: String,
    disposition: Option[String],
    contentId: Option[String]
  )

  implicit val sendgridAttachmentReads: Reads[SendgridAttachment] = (
    (JsPath \ "content").read[String] and
      (JsPath \ "type").readNullable[String] and
      (JsPath \ "filename").read[String] and
      (JsPath \ "disposition").readNullable[String] and
      (JsPath \ "content_id").readNullable[String]
    ) (SendgridAttachment.apply _)
  implicit val sendgridAttachmentWrites: Writes[SendgridAttachment] = (
    (JsPath \ "content").write[String] and
      (JsPath \ "type").writeNullable[String] and
      (JsPath \ "filename").write[String] and
      (JsPath \ "disposition").writeNullable[String] and
      (JsPath \ "content_id").writeNullable[String]
    ) (unlift(SendgridAttachment.unapply))

  /**
   * An object specifying how to handle unsubscribes.
   */
  case class SendgridAsm(
    groupId: Int,
    groupsToDisplay: Option[Vector[Int]]
  )

  implicit val sendgridAsmReads: Reads[SendgridAsm] = (
    (JsPath \ "group_id").read[Int] and
      (JsPath \ "groups_to_display").readNullable[Vector[Int]]
    ) (SendgridAsm.apply _)
  implicit val sendgridAsmWrites: Writes[SendgridAsm] = (
    (JsPath \ "group_id").write[Int] and
      (JsPath \ "groups_to_display").writeNullable[Vector[Int]]
    ) (unlift(SendgridAsm.unapply))

  /**
   * Whether blind carbon copies should be sent to a specific email.
   */
  case class SendgridBccSettings(
    enable: Option[Boolean],
    email: Option[String]
  )

  implicit val sendgridBccReads: Reads[SendgridBccSettings] = (
    (JsPath \ "enable").readNullable[Boolean] and
      (JsPath \ "email").readNullable[String]
    ) (SendgridBccSettings.apply _)
  implicit val sendgridBccSettingsWrites: Writes[SendgridBccSettings] = Json.writes[SendgridBccSettings]

  /**
   * Whether all unsubscribe groups and settings should be bypassed.
   */
  case class SendgridBypassListManagementSettings(
    enable: Option[Boolean]
  )

  implicit val sendgridBypassListManagementSettingsReads: Reads[SendgridBypassListManagementSettings] =
    (JsPath \ "enable").readNullable[Boolean].map(SendgridBypassListManagementSettings.apply)
  implicit val sendgridBypassListManagementSettingsWrites: Writes[SendgridBypassListManagementSettings] =
    Json.writes[SendgridBypassListManagementSettings]

  /**
   * The default footer appended to the bottom of every email.
   */
  case class SendgridFooterSettings(
    enable: Option[Boolean],
    text: Option[String],
    html: Option[String]
  )

  implicit val sendgridFooterSettingsReads: Reads[SendgridFooterSettings] = (
    (JsPath \ "enable").readNullable[Boolean] and
      (JsPath \ "text").readNullable[String] and
      (JsPath \ "html").readNullable[String]
    ) (SendgridFooterSettings.apply _)
  implicit val sendgridFooterSettingsWrites: Writes[SendgridFooterSettings] = Json.writes[SendgridFooterSettings]

  /**
   * Enables sandbox mode, allowing for test emails.
   */
  case class SendgridSandboxModeSettings(
    enable: Option[Boolean]
  )

  implicit val sendgridSandboxModeReads: Reads[SendgridSandboxModeSettings] =
    (JsPath \ "enable").readNullable[Boolean].map(SendgridSandboxModeSettings.apply)
  implicit val sendgridSandboxModeWrites: Writes[SendgridSandboxModeSettings] =
    Json.writes[SendgridSandboxModeSettings]

  /**
   * Enables testing for spam contents.
   */
  case class SendgridSpamCheckSettings(
    enable: Option[Boolean],
    threshold: Option[Int],
    postToUrl: Option[String]
  )

  implicit val sendgridSpamCheckSettingsReads: Reads[SendgridSpamCheckSettings] = (
    (JsPath \ "enable").readNullable[Boolean] and
      (JsPath \ "threshold").readNullable[Int] and
      (JsPath \ "post_to_url").readNullable[String]
    ) (SendgridSpamCheckSettings.apply _)
  implicit val sendgridSpamCheckSettingsWrites: Writes[SendgridSpamCheckSettings] = (
    (JsPath \ "enable").writeNullable[Boolean] and
      (JsPath \ "threshold").writeNullable[Int] and
      (JsPath \ "post_to_url").writeNullable[String]
    ) (unlift(SendgridSpamCheckSettings.unapply))

  /**
   * Various settings specifying how emails should be handled.
   */
  case class SendgridMailSettings(
    bcc: Option[SendgridBccSettings],
    bypassListManagement: Option[SendgridBypassListManagementSettings],
    footer: Option[SendgridFooterSettings],
    sandboxMode: Option[SendgridSandboxModeSettings],
    spamCheck: Option[SendgridSpamCheckSettings]
  )

  implicit val sendgridMailSettingsReads: Reads[SendgridMailSettings] = (
    (JsPath \ "bcc").readNullable[SendgridBccSettings] and
      (JsPath \ "bypass_list_management").readNullable[SendgridBypassListManagementSettings] and
      (JsPath \ "footer").readNullable[SendgridFooterSettings] and
      (JsPath \ "sandbox_mode").readNullable[SendgridSandboxModeSettings] and
      (JsPath \ "spam_check").readNullable[SendgridSpamCheckSettings]
    ) (SendgridMailSettings.apply _)
  implicit val sendgridMailSettingsWrites: Writes[SendgridMailSettings] = (
    (JsPath \ "bcc").writeNullable[SendgridBccSettings] and
      (JsPath \ "bypass_list_management").writeNullable[SendgridBypassListManagementSettings] and
      (JsPath \ "footer").writeNullable[SendgridFooterSettings] and
      (JsPath \ "sandbox_mode").writeNullable[SendgridSandboxModeSettings] and
      (JsPath \ "spam_check").writeNullable[SendgridSpamCheckSettings]
    ) (unlift(SendgridMailSettings.unapply))

  /**
   * Track whether a recipient has clicked a link in your email.
   */
  case class SendgridClickTrackingSettings(
    enable: Option[Boolean],
    enableText: Option[Boolean]
  )

  implicit val sendgridClickTrackingSettingsReads: Reads[SendgridClickTrackingSettings] = (
    (JsPath \ "enable").readNullable[Boolean] and
      (JsPath \ "enable_text").readNullable[Boolean]
    ) (SendgridClickTrackingSettings.apply _)
  implicit val sendgridClickTrackingSettingsWrites: Writes[SendgridClickTrackingSettings] = (
    (JsPath \ "enable").writeNullable[Boolean] and
      (JsPath \ "enableText").writeNullable[Boolean]
    ) (unlift(SendgridClickTrackingSettings.unapply))

  /**
   * Tracks whether an email has been opened.
   */
  case class SendgridOpenTrackingSettings(
    enable: Option[Boolean],
    substitutionTag: Option[String]
  )

  implicit val sendgridOpenTrackingSettingsReads: Reads[SendgridOpenTrackingSettings] = (
    (JsPath \ "enable").readNullable[Boolean] and
      (JsPath \ "substitution_tag").readNullable[String]
    ) (SendgridOpenTrackingSettings.apply _)
  implicit val sendgridOpenTrackingSettingsWrites: Writes[SendgridOpenTrackingSettings] = (
    (JsPath \ "enable").writeNullable[Boolean] and
      (JsPath \ "substitution_tag").writeNullable[String]
    ) (unlift(SendgridOpenTrackingSettings.unapply))

  /**
   * Adds a subscription management link to the email.
   */
  case class SendgridSubscriptionTrackingSettings(
    enable: Option[Boolean],
    text: Option[String],
    html: Option[String],
    substitutionTag: Option[String]
  )

  implicit val sendgridSubscriptionTrackingSettingsReads: Reads[SendgridSubscriptionTrackingSettings] = (
    (JsPath \ "enable").readNullable[Boolean] and
      (JsPath \ "text").readNullable[String] and
      (JsPath \ "html").readNullable[String] and
      (JsPath \ "substitution_tag").readNullable[String]
    ) (SendgridSubscriptionTrackingSettings.apply _)
  implicit val sendgridSubscriptionTrackingSettingsWrites: Writes[SendgridSubscriptionTrackingSettings] = (
    (JsPath \ "enable").writeNullable[Boolean] and
      (JsPath \ "text").writeNullable[String] and
      (JsPath \ "html").writeNullable[String] and
      (JsPath \ "substitution_tag").writeNullable[String]
    ) (unlift(SendgridSubscriptionTrackingSettings.unapply))

  /**
   * Enables email analytics.
   */
  case class SendgridGAnalyticsSettings(
    enable: Option[Boolean],
    utmSource: Option[String],
    utmMedium: Option[String],
    utmTerm: Option[String],
    utmContent: Option[String],
    utmCampaign: Option[String]
  )

  implicit val sendgridGAnalyticsSettingsReads: Reads[SendgridGAnalyticsSettings] = (
    (JsPath \ "enable").readNullable[Boolean] and
      (JsPath \ "utm_source").readNullable[String] and
      (JsPath \ "utm_medium").readNullable[String] and
      (JsPath \ "utm_term").readNullable[String] and
      (JsPath \ "utm_content").readNullable[String] and
      (JsPath \ "utm_campaign").readNullable[String]
    ) (SendgridGAnalyticsSettings.apply _)
  implicit val sendgridGAnalyticsSettingsWrites: Writes[SendgridGAnalyticsSettings] = (
    (JsPath \ "enable").writeNullable[Boolean] and
      (JsPath \ "utm_source").writeNullable[String] and
      (JsPath \ "utm_medium").writeNullable[String] and
      (JsPath \ "utm_term").writeNullable[String] and
      (JsPath \ "utm_content").writeNullable[String] and
      (JsPath \ "utm_campaign").writeNullable[String]
    ) (unlift(SendgridGAnalyticsSettings.unapply))

  /**
   * Settings to determine tracking email metrics.
   */
  case class SendgridTrackingSettings(
    clickTracking: Option[SendgridClickTrackingSettings],
    openTracking: Option[SendgridOpenTrackingSettings],
    subscriptionTracking: Option[SendgridSubscriptionTrackingSettings],
    ganalytics: Option[SendgridGAnalyticsSettings]
  )

  implicit val sendgridTrackingSettingsReads: Reads[SendgridTrackingSettings] = (
    (JsPath \ "click_tracking").readNullable[SendgridClickTrackingSettings] and
      (JsPath \ "open_tracking").readNullable[SendgridOpenTrackingSettings] and
      (JsPath \ "subscription_tracking").readNullable[SendgridSubscriptionTrackingSettings] and
      (JsPath \ "ganalytics").readNullable[SendgridGAnalyticsSettings]
    ) (SendgridTrackingSettings.apply _)
  implicit val sendgridTrackingSettingsWrites: Writes[SendgridTrackingSettings] = (
    (JsPath \ "click_tracking").writeNullable[SendgridClickTrackingSettings] and
      (JsPath \ "open_tracking").writeNullable[SendgridOpenTrackingSettings] and
      (JsPath \ "subscription_tracking").writeNullable[SendgridSubscriptionTrackingSettings] and
      (JsPath \ "ganalytics").writeNullable[SendgridGAnalyticsSettings]
    ) (unlift(SendgridTrackingSettings.unapply))

  /**
   * The body of a Sendgrid v3 email message.
   *
   * @see https://sendgrid.com/docs/API_Reference/Web_API_v3/Mail/index.html#-Request-Body-Parameters
   */
  case class SendgridEmail(
    personalizations: Vector[SendgridPersonalization],
    from: SendgridEmailObject,
    replyTo: Option[SendgridEmailObject],
    subject: Option[String],
    content: Option[Vector[SendgridContent]],
    attachment: Option[Vector[SendgridAttachment]],
    templateId: Option[String],
    sections: Option[JsValue],
    headers: Option[JsValue],
    categories: Option[Vector[String]],
    sendAt: Option[Int],
    batchId: Option[String],
    asm: Option[SendgridAsm],
    ipPoolName: Option[String],
    mailSettings: Option[SendgridMailSettings],
    trackingSettings: Option[SendgridTrackingSettings]
  )

  implicit val sendgridEmailReads: Reads[SendgridEmail] = (
    (JsPath \ "personalizations").read[Vector[SendgridPersonalization]] and
      (JsPath \ "from").read[SendgridEmailObject] and
      (JsPath \ "reply_to").readNullable[SendgridEmailObject] and
      (JsPath \ "subject").readNullable[String] and
      (JsPath \ "content").readNullable[Vector[SendgridContent]] and
      (JsPath \ "attachment").readNullable[Vector[SendgridAttachment]] and
      (JsPath \ "template_id").readNullable[String] and
      (JsPath \ "sections").readNullable[JsValue] and
      (JsPath \ "headers").readNullable[JsValue] and
      (JsPath \ "categories").readNullable[Vector[String]] and
      (JsPath \ "send_at").readNullable[Int] and
      (JsPath \ "batch_id").readNullable[String] and
      (JsPath \ "asm").readNullable[SendgridAsm] and
      (JsPath \ "ip_pool_name").readNullable[String] and
      (JsPath \ "mail_settings").readNullable[SendgridMailSettings] and
      (JsPath \ "tracking_settings").readNullable[SendgridTrackingSettings]
    ) (SendgridEmail.apply _)
  implicit val sendgridEmailWrites: Writes[SendgridEmail] = (
    (JsPath \ "personalizations").write[Vector[SendgridPersonalization]] and
      (JsPath \ "from").write[SendgridEmailObject] and
      (JsPath \ "reply_to").writeNullable[SendgridEmailObject] and
      (JsPath \ "subject").writeNullable[String] and
      (JsPath \ "content").writeNullable[Vector[SendgridContent]] and
      (JsPath \ "attachment").writeNullable[Vector[SendgridAttachment]] and
      (JsPath \ "template_id").writeNullable[String] and
      (JsPath \ "sections").writeNullable[JsValue] and
      (JsPath \ "headers").writeNullable[JsValue] and
      (JsPath \ "categories").writeNullable[Vector[String]] and
      (JsPath \ "send_at").writeNullable[Int] and
      (JsPath \ "batch_id").writeNullable[String] and
      (JsPath \ "asm").writeNullable[SendgridAsm] and
      (JsPath \ "ip_pool_name").writeNullable[String] and
      (JsPath \ "mail_settings").writeNullable[SendgridMailSettings] and
      (JsPath \ "tracking_settings").writeNullable[SendgridTrackingSettings]
    ) (unlift(SendgridEmail.unapply))
}