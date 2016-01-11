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

// java
import java.io.StringReader
import java.text.SimpleDateFormat

// scala
import scala.io.Source.fromInputStream

// akka
import akka.actor.{Props, ActorSystem, ActorRef}

// play
import play.api.libs.json.Json

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
 *
 * @param sendgrid Instance of Sendgrid.
 * @param logger LoggerActorWrapper for the wrapper.
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

        fromInputStream(is).getLines()
                           .toSeq
                           .map(extractWords)
                           .grouped(LINE_LIMIT) // respect Sendgrid's limitations
                           .foreach { case valuess =>
                             sendgrid.postRecipients(keys, valuess)
                             logger ! Notification("Successfully posted bunch of recipients to Sendgrid.")
                             Thread.sleep(WAIT_TIME) // note that for actor all messages come from single queue
                                                     // so new `fileAppeared` will be processed after current one
                           }

        true

      case _ => false
    }
  }
}

object Recipients {
  val LINE_LIMIT = 1000
  val WAIT_TIME = 3000L // 3 seconds

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
   * Created a Sendgrid-friendly json from given keys and valuess.
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
   * @return Sendgrid-friendly json.
   */
  def makeValidJson(keys: Seq[String], valuess: Seq[Seq[String]]): String = {
    val recipients = for (values <- valuess) yield {
      assert(values.length == keys.length) // todo check how that works
      val recipientDara = keys.zip(values)
      Json.toJson(recipientDara.toMap)
          .toString()
          .replaceAll(""""(\d+|null)"""", "$1") // null and all numbers too
    }

    s"[${recipients.mkString(",")}]"
  }

  /**
   * Converts given line in Sendgrid-friendly format.
   * That includes (see https://github.com/snowplow/sauna/wiki/SendGrid-responder-user-guide#214-response-algorithm):
   *   1) split the line into words
   *   2) change "" words to null
   *   3) change date-like words to epochs
   *
   * @param line A line to be converted.
   * @return A seq of corrected words
   */
  def extractWords(line: String): Seq[String] = {
    val reader = CSVReader.open(new StringReader(line))(tsvFormat)

    try {
      reader.readNext() // get next line, it should be only one
            .map(list => list.map(correctWord))
            .getOrElse(List())

    } catch {
      case _: Exception => List()

    } finally {
      reader.close()
    }
  }

  /**
   * Corrects a single word according to following rules:
   *   1) change "" words to null
   *   2) change timestamps words to epochs
   *
   * @see https://github.com/snowplow/sauna/wiki/SendGrid-responder-user-guide#214-response-algorithm
   *
   * @param word A word to be corrected.
   * @return Corrected word.
   */
  def correctWord(word: String): String = word match {
    case dateRegexpFull(timestamp) => dateFormatFull.parse(timestamp)
                                                .getTime
                                                ./(1000) // seems like Sendgrid does not accept milliseconds
                                                .toString
    case dateRegexpShort(timestamp) => dateFormatShort.parse(timestamp)
                                                     .getTime
                                                     ./(1000) // seems like Sendgrid does not accept milliseconds
                                                     .toString
    case "" => "null"
    case _ => word
  }
}