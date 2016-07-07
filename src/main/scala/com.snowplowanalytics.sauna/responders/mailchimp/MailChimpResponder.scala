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
package mailchimp

// java
import java.io.{ InputStream, StringReader }
import java.text.SimpleDateFormat

// scala
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source.fromInputStream

// akka
import akka.actor.{ ActorRef, Props }

// play
import play.api.libs.json._

// scala-csv
import com.github.tototoshi.csv._

// sauna
import apis.mailChimp
import loggers.Logger.Notification
import responders.Responder.FileAppeared
import utils._

/**
 * Does stuff for MailChimp import members feature.
 * @param mailchimp Instance of MailChimp.
 * @param logger A logger actor.
 */
class MailChimpResponder(mailchimp: mailChimp)(implicit logger: ActorRef) extends Responder {
  import MailChimpResponder._

  val pathPattern =
    """.*com\.mailchimp/
      |lists/
      |v1/
      |tsv:([^\/]+)/
      |.+$
    """.stripMargin
      .replaceAll("[\n ]", "")
  val pathRegexp = pathPattern.r

  override def process(fileAppeared: FileAppeared): Unit = {
    import fileAppeared._
    filePath match {
      case pathRegexp(attrs) =>
        if (attrs.isEmpty) {
          logger ! Notification("Should be at least one attribute.")
          return
        }

        if (!attrs.contains("list_id")) {
          logger ! Notification("Attribute 'list_id' must be included.")
          return
        }

        if (!attrs.contains("email_address")) {
          logger ! Notification("Attribute 'email_address' must be included.")
          return
        }

        if (!attrs.contains("status")) {
          logger ! Notification("Attribute 'status' must be included.")
          return
        }
        val keys = attrs.split(",")
        getData(is).foreach(processData(keys, _))
    }
  }

  /**
   * This method does the first part of job for "import members" feature.
   * It gets file content, parses it and splits into smaller chunks to satisfy Mailchimp's limitations.
   *
   * @param is InputStream to data file.
   * @return Iterator of valuess data. Valuess are extracted from the file.
   */
  def getData(is: InputStream): Iterator[Seq[Seq[String]]] = {
    fromInputStream(is).getLines()
      .toSeq
      .flatMap(valuesFromTsv)
      .grouped(LINE_LIMIT)
  }

  /**
   * This method does the second part of job for "import members" feature.
   *
   * @param keys Seq of attribute keys, repeated for each recipient from `valuess`.
   * @param valuess Seq of members, where member is a seq of attribute values.
   *                Each `values` in `valuess` should have one length with `keys`.
   */
  def processData(keys: Seq[String], valuess: Seq[Seq[String]]): Unit = {
    val (probablyValid, definitelyInvalid) = valuess.partition(_.length == keys.length)
    // deal with 100% corrupted data
    definitelyInvalid.foreach { invalidValues =>
      logger ! Notification(s"Unable to process [${invalidValues.mkString("\t")}], " +
        s"it has only ${invalidValues.length} columns, when ${keys.length} are required.")
    }

    val json = MailChimpResponder.makeValidJson(keys, probablyValid)
    mailchimp.uploadToMailChimpRequest(json)
    logger ! Notification("Upload Complete !")

    Thread.sleep(WAIT_TIME) // note that for actor all messages come from single queue
    // so new `fileAppeared` will be processed after current one
  }

}

object MailChimpResponder {
  val LINE_LIMIT = 1000
  val WAIT_TIME = 667L

  val whiteListedFields = List("email_type", "status", "interests", "language", "vip", "ip_signup", "timestamp_signup", "ip_opt", "timestamp_opt", "email_address", "location.latitude", "location.longitude", "location.gmtoff", "location.dstoff", "location.country_code", "location.timezone")
  val dateFormatFull = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
  val dateRegexpFull = "^(\\d{1,4}-\\d{1,2}-\\d{1,2} \\d{1,2}:\\d{1,2}:\\d{1,2}\\.\\d{1,3})$".r
  val dateFormatShort = new SimpleDateFormat("yyyy-MM-dd")
  val dateRegexpShort = "^(\\d{1,4}-\\d{1,2}-\\d{1,2})$".r

  /**
   * Constructs a Props for MailChimpResponder actor.
   *
   * @param mailchimp	 Instance of Mailchimp.
   * @param logger Actor with underlying Logger.
   * @return Props for new actor.
   */
  def apply(mailchimp: mailChimp)(implicit logger: ActorRef): Props =
    Props(new MailChimpResponder(mailchimp))

  /**
   * Creates a MailChimp-friendly json from given keys and valuess.
   *
   * @param keys Seq of attribute keys, repeated for each recipient from `valuess`.
   * @param valuess Seq of recipients, where recipient is a seq of attribute values.
   *                Each `values` in `valuess` must already have one length with `keys`.
   * @return MailChimp-friendly json.
   * @see https://github.com/snowplow/sauna/wiki/MailChimp-responder-user-guide#214-response-algorithm
   */
  def makeValidJson(keys: Seq[String], valuess: Seq[Seq[String]]): String = {
    val recipients = for (
      values <- valuess;
      _ = assert(values.length == keys.length);
      correctedValues = values.map(correctTimestamps)
    ) yield {
      val recipientsData = keys.zip(correctedValues)
      val (listName, bodyList) = fieldsAllowed(recipientsData)
      val bodyJson = Json.toJson(bodyList.toMap)
      val emailId = (bodyJson \ "email_address").as[String]
      val body = bodyJson.toString()
        .replaceAll("\"\"", "null") // null should be without quotations
        .replaceAll(""""(\d+)"""", "$1") // and positive integers too
      val path = "lists/" + listName + "/members"
      (Json.obj("method" -> "POST") ++ Json.obj("path" -> path) ++ Json.obj("operation_id" -> (listName + "_" + emailId)) ++ Json.obj("body" -> body))
    }
    val mailchimpBatchJson = Json.obj("operations" -> recipients)
    mailchimpBatchJson.toString
  }

  /**
   * gets the Seq of key value pairs and convert it to a Seq of only whitelisted fields and rest as merge fields
   * @param data Seq of key value pairs
   * @return tuple of listId and the Seq of key value pairs with only Mailchimp whitelisted fields and rest as merge fields
   **/
  
  def fieldsAllowed(data: Seq[(String, String)]): (String, Seq[(String, String)]) = { 
    val (list1, list2) = data.partition(x => whiteListedFields.contains(x._1) == true)
    val (listTuple, mergeList) = list2.partition(x => (x._1 == "list_id"))
    val bodyList = if (mergeList.length > 0) {
      list1 :+ ("merge_fields" -> Json.toJson(mergeList.toMap).toString)
    } else {
      list1
    }
    (listTuple(0)._2, bodyList)
  }

  /**
   * Tries to extract values from given tab-separated line.
   *
   * @param tsvLine A tab-separated line.
   * @return Some[Seq of values]. In case of exception, returns None.
   */
  def valuesFromTsv(tsvLine: String): Option[Seq[String]] = {
    val reader = CSVReader.open(new StringReader(tsvLine))(tsvFormat)
    try {
      reader.readNext() // get next line, it should be only one
    } catch {

      case _: Exception => None

    } finally {
      reader.close()
    }
  }

  /**
   * Corrects a single string according to following rules:
   *   1) change timestamps words to epochs
   *
   * @see https://github.com/snowplow/sauna/wiki/MailChimp-responder-user-guide
   * @param s A string to be corrected.
   * @return Corrected word.
   */
  def correctTimestamps(s: String): String = s match {
    case dateRegexpFull(timestamp) => dateFormatFull.parse(timestamp)
      .getTime
      ./(1000) // seems like MailChimp does not accept milliseconds
      .toString
    case dateRegexpShort(timestamp) => dateFormatShort.parse(timestamp)
      .getTime
      ./(1000) // seems like MailChimp does not accept milliseconds
      .toString
    case _ => s
  }
}
