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

/**
  * Represents input data format + helper methods.
  */
// todo use macros to generate this class
case class TargetingList(projectId: String, listName: String, listDescription: String, listType: Short, keyFields: Option[String], value: String)

object TargetingList extends PartialFunction[String, TargetingList] {
  // todo use something faster than regexp (can be done in single pass)
  val r = """(.+?)\t(.+?)\t(.+?)\t([0-9]+?)\t(.*?)\t(.+?)""".r

  def unapply(line: String): Option[TargetingList] = line match {
    case r(projectId, listName, listDescription, _listType, _keyFields, value) =>
      val listType = _listType.toShort
      val keyFields = if (_keyFields.isEmpty) None else Some(_keyFields)

      Some(TargetingList(projectId, listName, listDescription, listType, keyFields, value))

    case _ => None
  }

  override def isDefinedAt(s: String): Boolean = unapply(s).isDefined // todo remove double work here (memoization?)

  override def apply(s: String): TargetingList = unapply(s).get       // and here

  /**
    * Helper method, that converts several TargetingLists in Optimizely-friendly format.
    *
    * @param tls list of TargetingLists.
    * @return a String in Optimizely-friendly format.
    */
  // todo should be generated too
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
