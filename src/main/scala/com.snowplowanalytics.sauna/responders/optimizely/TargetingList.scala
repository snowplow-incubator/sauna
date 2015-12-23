/*
 * Copyright (c) 2015 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.sauna.responders.optimizely

import java.io.InputStream
import scala.io.Source.fromInputStream

import com.snowplowanalytics.sauna.Sauna

/**
  * Represents input data format + helper methods.
  */
case class TargetingList(projectId: String, listName: String, listDescription: String, listType: Short, keyFields: Option[String], value: String)

object TargetingList {
  val pathPattern = """.*com\.optimizely\/targeting_lists\/v1\/tsv:\*\/.*$"""
  val validLineRegexp = """(.+?)\t(.+?)\t(.+?)\t([0-9]+?)\t(.*?)\t(.+?)""".r

  def unapply(line: String): Option[TargetingList] = line match {
    case validLineRegexp(projectId, listName, listDescription, _listType, _keyFields, value) =>
      val listType = _listType.toShort
      val keyFields = if (_keyFields.isEmpty) None else Some(_keyFields)

      Some(TargetingList(projectId, listName, listDescription, listType, keyFields, value))

    case _ => None
  }

  def process(filePath: String, is: InputStream): PartialFunction[(String, InputStream), Unit] = {
    val matches = filePath.matches(pathPattern)

    new PartialFunction[(String, InputStream), Unit] {
      override def isDefinedAt(x: (String, InputStream)): Boolean = matches

      override def apply(v1: (String, InputStream)): Unit =
        fromInputStream(is).getLines()
                           .toSeq
                           .flatMap(s => TargetingList.unapply(s))
                           .groupBy(t => (t.projectId, t.listName)) // https://github.com/snowplow/sauna/wiki/Optimizely-responder-user-guide#215-troubleshooting
                           .foreach { case (_, tls) => Sauna.optimizely.targetingLists(tls) }
    }
  }

  /**
    * Helper method, that converts several TargetingLists in Optimizely-friendly format.
    *
    * @param tls list of TargetingLists.
    * @return a String in Optimizely-friendly format.
    */
  def merge(tls: Seq[TargetingList]): String = {
    val head = tls.head
    val name = s""""${head.listName}""""
    val description = s""""${head.listDescription}""""
    val list_type = head.listType
    val key_fields = head.keyFields
                         .map(kf => s""""$kf"""")
                         .getOrElse("null")
    val values = tls.map(_.value)
                    .mkString(",")
    val list_content = s""""$values""""
    val format = """"tsv""""

    s"""{"name":$name, "description":$description, "list_type":$list_type, "key_fields":$key_fields, "list_content":$list_content,"format":$format}"""
  }
}
