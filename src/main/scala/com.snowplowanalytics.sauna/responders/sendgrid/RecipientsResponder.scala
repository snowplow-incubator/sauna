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
import java.io.{InputStream, StringReader}

// scala
import scala.io.Source.fromInputStream
import scala.util.control.NonFatal

// akka
import akka.actor.{ActorRef, Props}

// scala-csv
import com.github.tototoshi.csv._

// sauna
import RecipientsResponder._
import RecipientsWorker.Delegate
import apis.Sendgrid
import observers.Observer._
import responders.Responder._
import utils._


/**
 * Actor extracting `RecipientsPublished` event from published file and passing
 * necessary information to worker actor
 *
 * @see https://sendgrid.com/docs/User_Guide/Marketing_Campaigns/contacts.html
 * @see https://github.com/snowplow/sauna/wiki/SendGrid-responder-user-guide
 * @param sendgrid Sendgrid API Wrapper
 * @param logger   A logger actor.
 */
class RecipientsResponder(sendgrid: Sendgrid, val logger: ActorRef) extends Responder[ObserverFileEvent, RecipientsPublished] {

  /**
   * Worker actor doing all posting
   */
  val worker = context.actorOf(RecipientsWorker.props(sendgrid))

  /**
   * Extract [[RecipientsPublished]] from path like:
   * `com.sendgrid.contactdb/recipients/v1/tsv:email,customerId,whenCreated/ua-team/joe/warehouse.tsv`
   * Where attributes after `tsv:` should denote column names in `warehouse.tsv`
   *
   * @param observerEvent some event sent from observer
   * @return Some [[RecipientsPublished]] if this observer-event need to be
   *         processed by this responder, None if event need to be skept
   */
  def extractEvent(observerEvent: ObserverEvent): Option[RecipientsPublished] = {
    observerEvent match {
      case e: ObserverFileEvent =>
        e.id match {
          case pathRegexp(attrs) if attrs.split(",").contains("email") =>
            Some(RecipientsPublished(attrs.split(",").toList, e))
          case pathRegexp(_) =>
            notify(s"RecipientsResponder: attribute 'email' for [${observerEvent.id}] must be included")
            None
          case _ => None
        }
      case _ => None
    }
  }

  /**
   * Try to delegate all work to worker actor
   */
  def process(event: RecipientsPublished): Unit = {
    event.source.streamContent match {
      case Some(content) =>
        worker ! Delegate(RecipientsChunks.parse(content, event, notify _))
      case None =>
        notify(s"FAILURE: event's [${event.source.id}] source doesn't exist")
    }
  }
}

object RecipientsResponder {
  /**
   * Sendgrid limitation for amount of individual recipient per HTTP request.
   * Basis for chunking
   *
   * @see https://sendgrid.com/docs/API_Reference/Web_API_v3/Marketing_Campaigns/contactdb.html#Add-a-Single-Recipient-to-a-List-POST
   */
  val LINE_LIMIT = 1000

  /**
   * Deeply nested structure representing 1000-line chunks of tab-separated lines
   */
  type TsvChunks = Chunks[Lines[TSV]]

  // Parts of TsvChunks
  type Chunks[+A] = Iterator[A]
  type Lines[+A] = Seq[A]
  type TSV = List[String]

  /**
   * Regular expression allowing to extract TSV attributes from file root
   */
  val pathPattern =
    """.*com\.sendgrid\.contactdb/
      |recipients/
      |v1/
      |tsv:([^\/]+)/
      |.+$
    """.stripMargin.replaceAll("[\n ]", "")

  val pathRegexp = pathPattern.r

  /**
   * Recipients-file published event
   *
   * @param attrs  list of attributes extracted from filepath
   * @param source underlying observer event
   */
  case class RecipientsPublished(
    attrs: List[String],
    source: ObserverFileEvent
  ) extends ResponderEvent

  /**
   * Constructs a Props for RecipientsResponder actor
   *
   * @param sendgrid Instance of Sendgrid
   * @param logger   Actor with underlying Logger
   * @return Props for new actor
   */
  def props(logger: ActorRef, sendgrid: Sendgrid): Props =
    Props(new RecipientsResponder(sendgrid, logger))

  /**
   * List of fields that are reserved in Contacts DB and *can* (`email` *must*)
   * be included in attributes
   */
  val reservedFields = List(
    Sendgrid.CustomType(0, "email", Sendgrid.SendgridText),
    Sendgrid.CustomType(0, "first_name", Sendgrid.SendgridText),
    Sendgrid.CustomType(0, "last_name", Sendgrid.SendgridText)
  )

  /**
   * Lazy structure holding **mutable** chunks iterator and accompanying list
   * of attributes for TSV-columns in chunks. Each line in `chunkIterator`
   * guaranteed to have correct amount of columns (same as `attrs`).
   * TSV data grouped into chunks because Sendgrid can accept payloads limited
   * to `LINE_LIMIT`
   *
   * @param source        immutable responder event responsible for this chunks
   * @param chunkIterator mutable iterator of chunks (according to `MAX_LINES`)
   */
  private[sendgrid] class RecipientsChunks private(
    val source: RecipientsPublished,
    val chunkIterator: TsvChunks
  ) extends Serializable {

    /**
     * Information about types of custom fields
     */
    var customFields: Option[Sendgrid.CustomTypes] = None

    /**
     * Order fields fetched from Sendgrid API according to their order in
     * observer event attributes. If some fields appear in `ct`, but not in
     * `attributes` they will be thrown away
     *
     * @param ct Sendgrid information about field types
     * @return successfully reordered `CustomTypes` if all columns in `attribtues`
     *         are known. Error if `attributes` has unknown fields
     */
    def orderFields(ct: Sendgrid.CustomTypes): Either[String, Sendgrid.CustomTypes] = {
      val allFields = ct.copy(customTypes = ct.customTypes ++ reservedFields)
      val reorderedCustomFields = source.attrs.foldLeft(RecipientsChunks.init) { (acc, cur) =>
        acc match {
          case Right(types) =>
            allFields.customTypes.find(_.name == cur) match {
              case Some(t) => Right(t :: types)
              case None => Left(s"Field [$cur] is unknown")
            }
          case Left(error) => Left(error)
        }
      }
      reorderedCustomFields match {
        case Right(fields) => Right(Sendgrid.CustomTypes(fields.reverse, ordered = true))
        case Left(error) => Left(error)
      }
    }
  }

  object RecipientsChunks {
    /**
     * The only possible way to create `RecipientsChunks` object. Using `parse`
     * we can be sure that underlying iterator always produces lines with
     * correct amount of columns
     *
     * @param is        stream of bytes for underlying source of data (file or S3 object)
     * @param event     responder event containing all information, including attributes
     * @param onInvalid callback to notify system about unexpected line
     * @return `RecipientsChunks` with underlying iterator containing only valid lines
     */
    def parse(is: InputStream, event: RecipientsPublished, onInvalid: String => Unit): RecipientsChunks = {
      val lineSize = event.attrs.length

      val lines: Iterator[TSV] = fromInputStream(is).getLines.flatMap { line =>
        valuesFromTsv(line) match {
          case Some(tsv) if tsv.length == lineSize => Some(tsv)
          case Some(_) =>
            onInvalid(s"RecipientsResponder: line length unmatch [$line]")
            None
          case None =>
            onInvalid(s"RecipientsResponder: incorrect line [$line]")
            None
        }
      }

      new RecipientsChunks(event, lines.grouped(LINE_LIMIT))
    }

    private val init: Either[String, List[Sendgrid.CustomType]] = Right(Nil)
  }

  /**
   * Tries to extract values from given tab-separated line.
   *
   * @param tsvLine A tab-separated line.
   * @return Some[Seq of values]. In case of exception, returns None.
   */
  def valuesFromTsv(tsvLine: String): Option[List[String]] = {
    val reader = CSVReader.open(new StringReader(tsvLine))(tsvFormat)

    try {
      reader.readNext() // get next line, it should be only one
    } catch {
      case NonFatal(_) => None
    } finally {
      reader.close()
    }
  }
}