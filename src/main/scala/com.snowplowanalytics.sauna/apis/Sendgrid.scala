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
package apis

// scala
import scala.concurrent.Future
import scala.util.Try

// nscala-time
import com.github.nscala_time.time.StaticDateTimeFormat

// play
import play.api.libs.json._
import play.api.libs.functional.syntax._
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
    )(CustomType.apply _)

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
}