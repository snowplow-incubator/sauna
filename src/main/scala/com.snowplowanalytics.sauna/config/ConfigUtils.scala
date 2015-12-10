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
package config

// java-io
import java.io._

// play-json
import play.api.libs.json._

object ConfigUtils {
  /**
   * Traverses the json with seq of paths.
   * Example: val jsValue = Json.parse("""{ "a" : { "b" : "c" } }""")
   *          val lookupResult = lookup(jsValue, Seq("a", "b")) // => Some("c")
   *
   * @param json Initial json to start seek.
   * @param paths Seq of paths keys, that leads to the sought value (in successful case).
   *
   * @return Result of "moving by" `paths` over `json`.
   */
  def findInJson(json: JsValue, paths: Seq[String]): Option[JsValue] =
    if (paths.isEmpty) Some(json)
    else paths.tail
              .foldLeft(json \ paths.head)((res, x) => res \ x )
              .toOption

  /**
   * Collects all appropriate jsons for given Config, using `name` under `namePath` as filter feature.
   *
   * @param directory A directory where configuration files are located.
   *                  Note that search is not recursive.
   * @param name Distinctive feature of all configuration files for some Config
   *             (one Config may be merged from different files).
   *             Files in directory will be scanned, and those who has a 'name' under 'namePath' will process
   *             to next step - extracting values to form a Config.
   * @param namePath Where `name` is expected to be located.
   *
   * @return Seq of all files (as json) that have a 'name' under 'namePath', using file's content as json.
   */
  def configurationFiles(directory: String, name: String, namePath: Seq[String] = Seq("data", "name")): Seq[JsValue] =
    new File(directory).listFiles()
                       .flatMap { case file =>
                         try {
                           val inputStream = new FileInputStream(file)
                           val json = Json.parse(inputStream)
                           findInJson(json, namePath).map(_.as[String]) match {
                             case Some(value) => if (value == name) Some(json) else None
                             case None => None
                           }

                         } catch { case e: Exception =>
                           None
                         }
                       }

  /**
   * Tries to find a value by given `paths` over all `jsons`.
   * First successful result wins. Non-determinism by design.
   *
   * @param jsons Seq of json where lookups will be performed.
   * @param paths Seq of paths keys, that leads to the sought value (in successful case).
   *
   * @return Option(desired value).
   */
  def findInJsons(jsons: Seq[JsValue], paths: Seq[String]): Option[JsValue] =
    jsons.view // lazy eval & map, so no double calculation happens
         .flatMap(findInJson(_, paths))
         .take(1)
         .force
         .headOption
}