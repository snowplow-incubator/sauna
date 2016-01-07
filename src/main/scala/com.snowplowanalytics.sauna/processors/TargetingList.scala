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

// scala
import scala.io.Source.fromInputStream

// akka
import akka.actor.{Props, ActorSystem, ActorRef}

// sauna
import apis.Optimizely
import processors.Processor.FileAppeared

/**
 * Does stuff for Optimizely Targeting List feature.
 *
 * @param optimizely Instance of Optimizely.
 * @param logger LoggerActorWrapper for the wrapper.
 */
class TargetingList(optimizely: Optimizely)
                   (implicit logger: ActorRef) extends Processor {
  import TargetingList._

  override def process(fileAppeared: FileAppeared): Unit = {
    import fileAppeared._

    if (filePath.matches(pathPattern)) {
      fromInputStream(is).getLines()
                         .toSeq
                         .flatMap(s => TargetingList.unapply(s))
                         .groupBy(t => (t.projectId, t.listName)) // https://github.com/snowplow/sauna/wiki/Optimizely-responder-user-guide#215-troubleshooting
                         .foreach { case (_, tls) => optimizely.postTargetingLists(tls) }
    }
  }
}

object TargetingList {
  val pathPattern =
    """.*com\.optimizely/
      |targeting_lists/
      |v1/
      |tsv:\*/
      |.*$
    """.stripMargin
       .replaceAll("[\n ]", "")

  val validLineRegexp = """(.+?)\t(.+?)\t(.+?)\t([0-9]+?)\t(.*?)\t(.+?)""".r

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
  def unapply(line: String): Option[Data] = line match {
    case validLineRegexp(projectId, listName, listDescription, _listType, _keyFields, value) =>
      val listType = _listType.toShort
      val keyFields = if (_keyFields.isEmpty) None else Some(_keyFields)

      Some(Data(projectId, listName, listDescription, listType, keyFields, value))

    case _ => None
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