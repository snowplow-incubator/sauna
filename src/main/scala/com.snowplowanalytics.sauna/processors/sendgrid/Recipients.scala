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
package processors
package sendgrid

// java
import java.io.{InputStream, StringReader}
import java.text.SimpleDateFormat

// scala
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source.fromInputStream

// akka
import akka.actor.{ActorRef, ActorSystem, Props}

// play
import play.api.libs.json._

// scala-csv
import com.github.tototoshi.csv._

// sauna
import apis.Sendgrid
import loggers.Logger.Notification
import processors.Processor.FileAppeared

/**
 * Does stuff for Sendgrid import recipients feature.
 *
 * @see https://sendgrid.com/docs/User_Guide/Marketing_Campaigns/contacts.html
 * @see https://github.com/snowplow/sauna/wiki/SendGrid-responder-user-guide
 * @param sendgrid Instance of Sendgrid.
 * @param logger A logger actor.
 */
class Recipients(sendgrid: Sendgrid)
                (implicit logger: ActorRef) extends Processor {
  import Recipients._

  override def processed(fileAppeared: FileAppeared): Boolean = {
    import fileAppeared._

    filePath match {
      case pathRegexp(attrs) => // file is subject of Recipients processor, so return true now
        if (attrs.isEmpty) {
          logger ! Notification("Should be at least one attribute.")
          return true
        }

        if (!attrs.contains("email")) {
          logger ! Notification("Attribute 'email' must be included.")
          return true
        }

        val keys = attrs.split(",")

        // do the stuff
        getData(is).foreach(processData(keys, _))

        true

      case _ => false
    }
  }

  /**
   * This method does the first part of job for "import recipients" feature.
   * It gets file content, parses it and splits into smaller chunks to satisfy Sendgrid's limitations.
   *
   * @param is InputStream to data file.
   * @return Iterator of valuess data. Valuess are extracted from the file.
   * @see `Recipients.makeValidJson`
   */
  def getData(is: InputStream): Iterator[Seq[Seq[String]]] = {
    fromInputStream(is).getLines()
                       .toSeq
                       .flatMap(valuesFromTsv)
                       .grouped(LINE_LIMIT)
  }

  /**
   * This method does the second part of job for "import recipients" feature.
   * It handles errors and sends result to Sendgrid.
   *
   * @param keys Seq of attribute keys, repeated for each recipient from `valuess`.
   * @param valuess Seq of recipients, where recipient is a seq of attribute values.
   *                Each `values` in `valuess` should have one length with `keys`.
   * @see `Recipients.makeValidJson`
   */
  def processData(keys: Seq[String], valuess: Seq[Seq[String]]): Unit = {
    val (probablyValid, definitelyInvalid) = valuess.partition(_.length == keys.length)

    // deal with 100% corrupted data
    definitelyInvalid.foreach { invalidValues =>
      logger ! Notification(s"Unable to process [${invalidValues.mkString("\t")}], " +
                            s"it has only ${invalidValues.length} columns, when ${keys.length} are required.")
    }

    val json = Recipients.makeValidJson(keys, probablyValid)
    sendgrid.postRecipients(json)
            .foreach { case response =>
              val json = response.body
              handleErrors(probablyValid.length, json)
            }

    Thread.sleep(WAIT_TIME) // note that for actor all messages come from single queue
                            // so new `fileAppeared` will be processed after current one
  }

  /**
   * Used to handle possible errors from Sendgrid's response.
   * Informs user via logger ! Notification.
   * Heavily relies to Sendgrid's response json structure.
   *
   * @param totalRecordsNumber Sometimes records "disappear".
   *                           So, originally were 10, error_count = 2, updated_count = 4, new_count = 3.
   *                           One record was lost. This param is expected total records number.
   * @param jsonText A json text from Sendgrid.
   */
  def handleErrors(totalRecordsNumber: Int, jsonText: String) = try {
    val json = Json.parse(jsonText)
    /*
      json example:

      {
         "error_count":1,
         "error_indices":[
            2
         ],
         "errors":[
            {
               "error_indices":[
                  2
               ],
               "message":"date type conversion error"
            }
         ],
         "new_count":2,
         "persisted_recipients":[
            "Ym9iQGZvby5jb20=",
            "a2FybEBiYXIuZGU="
         ],
         "updated_count":0
      }

      */
    val errorCount = (json \ "error_count").as[Int]
    val errorIndices = (json \ "error_indices").as[Seq[Int]]
    val errors = (json \ "errors").as[Seq[JsObject]]
                                  .map(_.value)
    val newCount = (json \ "new_count").as[Int]
    val updatedCount = (json \ "updated_count").as[Int]

    for (errorIndex <- errorIndices) { // trying to get error explanation
      errors.find(_.apply("error_indices")
                   .as[Seq[Int]]
                   .contains(errorIndex)) match {
        case Some(error) =>
          val reason = error.apply("message")
                            .as[String]
          logger ! Notification(s"Error $errorIndex caused due to [$reason].")

        case None =>
          logger ! Notification(s"Unable to find reason for error $errorIndex.")
      }
    }

    if (errorCount + newCount + updatedCount != totalRecordsNumber) {
      logger ! Notification(s"For some reasons, several records disappeared. " +
                            s"It's rare Sendgrid's bug double-check you input. ")
    }

  } catch { case e: Exception =>
    logger ! Notification(s"Got exception ${e.getMessage} while parsing Sendgrid's response.")
  }
}

object Recipients {
  val LINE_LIMIT = 1000 // https://sendgrid.com/docs/API_Reference/Web_API_v3/Marketing_Campaigns/contactdb.html#Add-a-Single-Recipient-to-a-List-POST
  val WAIT_TIME = 667L // https://sendgrid.com/docs/API_Reference/Web_API_v3/Marketing_Campaigns/contactdb.html#Add-Recipients-POST

  val pathRegexp =
    """.*com\.sendgrid\.contactdb/
      |recipients/
      |v1/
      |tsv:([^\/]+)/
      |.+$
    """.stripMargin
       .replaceAll("[\n ]", "")
       .r

  val tsvFormat = new TSVFormat {} // force scala-csv to use tsv
  val dateFormatFull = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
  val dateRegexpFull = "^(\\d{1,4}-\\d{1,2}-\\d{1,2} \\d{1,2}:\\d{1,2}:\\d{1,2}\\.\\d{1,3})$".r
  val dateFormatShort = new SimpleDateFormat("yyyy-MM-dd")
  val dateRegexpShort = "^(\\d{1,4}-\\d{1,2}-\\d{1,2})$".r

  /**
   * Constructs a Recipients actor.
   *
   * @param sendgrid Instance of Sendgrid.
   * @param system Actor system that creates an actor.
   * @param logger Actor with underlying Logger.
   * @return Recipients as ActorRef.
   */
  def apply(sendgrid: Sendgrid)(implicit system: ActorSystem, logger: ActorRef): ActorRef =
    system.actorOf(Props(new Recipients(sendgrid)))

  /**
   * Creates a Sendgrid-friendly json from given keys and valuess.
   *
   * For example, for `keys` = Seq("name1", "name2"),
   *                  `valuess` = Seq(Seq("value11", "value12"), Seq("value21", "value22")),
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
   * @param keys Seq of attribute keys, repeated for each recipient from `valuess`.
   * @param valuess Seq of recipients, where recipient is a seq of attribute values.
   *                Each `values` in `valuess` must already have one length with `keys`.
   * @return Sendgrid-friendly json.
   * @see https://github.com/snowplow/sauna/wiki/SendGrid-responder-user-guide#214-response-algorithm
   */
  def makeValidJson(keys: Seq[String], valuess: Seq[Seq[String]]): String = {
    val recipients = for (values <- valuess;
                          _ = assert(values.length == keys.length);
                          correctedValues = values.map(correctTimestamps))
                       yield {
                         val recipientsData = keys.zip(correctedValues)
                         Json.toJson(recipientsData.toMap)
                             .toString()
                             .replaceAll("\"\"", "null") // null should be without quotations
                             .replaceAll(""""(\d+)"""", "$1") // and positive integers too
                       }

    s"[${recipients.mkString(",")}]"
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
   * @see https://github.com/snowplow/sauna/wiki/SendGrid-responder-user-guide#214-response-algorithm
   * @param s A string to be corrected.
   * @return Corrected word.
   */
  def correctTimestamps(s: String): String = s match {
    case dateRegexpFull(timestamp) => dateFormatFull.parse(timestamp)
                                                    .getTime
                                                    ./(1000) // seems like Sendgrid does not accept milliseconds
                                                    .toString
    case dateRegexpShort(timestamp) => dateFormatShort.parse(timestamp)
                                                      .getTime
                                                      ./(1000) // seems like Sendgrid does not accept milliseconds
                                                      .toString
    case _ => s
  }
}