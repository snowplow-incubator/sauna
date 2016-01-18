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
package optimizely

// java
import java.io.StringReader
import java.util.UUID

// scala
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source.fromInputStream

// akka
import akka.actor.{ActorRef, ActorSystem, Props}

// play
import play.api.libs.json.Json

// jackson
import com.fasterxml.jackson.core.JsonParseException

// scala-csv
import com.github.tototoshi.csv._

// sauna
import apis.Optimizely
import loggers.Logger.{Notification, Manifestation}
import processors.Processor.FileAppeared

/**
 * Does stuff for Optimizely Targeting List feature.
 * @see https://github.com/snowplow/sauna/wiki/Optimizely-responder-user-guide#targeting-list
 *
 * @param optimizely Instance of Optimizely.
 * @param logger A logger actor.
 */
class TargetingList(optimizely: Optimizely)
                   (implicit logger: ActorRef) extends Processor {
  import TargetingList._

  override def processed(fileAppeared: FileAppeared): Boolean = {
    import fileAppeared._

    if (filePath.matches(pathPattern)) {
      fromInputStream(is).getLines()
                         .toSeq
                         .flatMap(s => TargetingList.unapply(s)) // create TargetingList.Data from each line
                         .groupBy(t => (t.projectId, t.listName)) // https://github.com/snowplow/sauna/wiki/Optimizely-responder-user-guide#215-troubleshooting
                         .map { case (_, tls) => optimizely.postTargetingLists(tls) } // for each group make an upload
                         .foreach { future => future.foreach { case response => // parse each response to do logging
                           lazy val defaultId = UUID.randomUUID().toString
                           lazy val defaultName = "Not found."
                           lazy val defaultDescription = response.body
                           lazy val defaultLastModified = new java.sql.Timestamp(System.currentTimeMillis).toString
                           val status = response.status

                           try { // response.body is valid json
                             val json = Json.parse(response.body)
                             val id = (json \ "id").asOpt[Long]
                                                   .orElse((json \ "uuid").asOpt[String])
                                                   .getOrElse(defaultId)
                             val name = (json \ "name").asOpt[String]
                                                       .getOrElse(defaultName)
                             val description = (json \ "description").asOpt[String]
                                                                     .orElse((json \ "message").asOpt[String])
                                                                     .getOrElse(defaultDescription)
                             val lastModified = (json \ "last_modified").asOpt[String]
                                                                        .getOrElse(defaultLastModified)

                             // log results
                             logger ! Manifestation(id.toString, name, status, description, lastModified)
                             if (status == 201) {
                               logger ! Notification(s"Successfully uploaded targeting lists with name [$name].")
                             } else {
                               logger ! Notification(s"Unable to upload targeting list with name [$name] : [${response.body}].")
                             }

                           } catch { case e: JsonParseException =>
                             logger ! Manifestation(defaultId, defaultName, status, defaultDescription, defaultLastModified)
                             logger ! Notification(s"Problems while connecting to Optimizely API. See [${response.body}].")
                           }
                         }}
      true // file was processed
    } else {
      false // file was not processed
    }
  }
}

object TargetingList {
  val pathPattern =
    """.*com\.optimizely/
      |targeting_lists/
      |v1/
      |tsv:\*/
      |.+$
    """.stripMargin
       .replaceAll("[\n ]", "")

  val tsvFormat = new TSVFormat {} // force scala-csv to use tsv

  /**
   * Represents valid line format.
   */
  case class Data(projectId: String, listName: String, listDescription: String,
                  listType: Short, keyFields: Option[String], value: String)

  /**
   * Constructs a TargetingList actor.
   *
   * @param optimizely Instance of Optimizely.
   * @param system Actor system that creates an actor.
   * @param logger Actor with underlying Logger.
   * @return TargetingList as ActorRef.
   */
  def apply(optimizely: Optimizely)(implicit system: ActorSystem, logger: ActorRef): ActorRef =
    system.actorOf(Props(new TargetingList(optimizely)))

  /**
   * Tries to extract an Data from given string.
   *
   * @param line A string to be extracting from.
   * @return Option[Data]
   */
  def unapply(line: String): Option[Data] = {
    val reader = CSVReader.open(new StringReader(line))(tsvFormat)

    try {
      reader.readNext() match {
        case Some(List(projectId: String, listName: String, listDescription: String, _listType: String, _keyFields: String, value: String)) =>
            val listType = _listType.toShort
            val keyFields = if (_keyFields.isEmpty) None else Some(_keyFields)
            Some(Data(projectId, listName, listDescription, listType, keyFields, value))

        case _ => None
      }

    } catch {
      case _: Exception => None

    } finally {
      reader.close()
    }
  }

  /**
   * Helper method, that converts several TargetingLists in Optimizely-friendly format.
   *
   * @param tlData list of TargetingLists.Data.
   * @return a String in Optimizely-friendly format.
   */
  def merge(tlData: Seq[Data]): String = {
    val head = tlData.head
    val name = s""""${head.listName}""""
    val description = s""""${head.listDescription}""""
    val list_type = head.listType
    val key_fields = head.keyFields
                         .map(kf => s""""$kf"""")
                         .getOrElse("null")
    val values = tlData.map(_.value)
                       .mkString(",")
    val list_content = s""""$values""""
    val format = """"tsv""""

    s"""{"name":$name, "description":$description, "list_type":$list_type, "key_fields":$key_fields, "list_content":$list_content,"format":$format}"""
  }
}