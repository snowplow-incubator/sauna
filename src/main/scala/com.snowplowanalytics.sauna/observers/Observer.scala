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
package com.snowplowanalytics.sauna.observers

/**
  * After new file appeared, Sauna should process it somehow.
  * Implementations of this trait can watch for "a place were new files can appear".
  */
trait Observer {
  /**
    * Main method, that describes "how to start watching for some area".
    *
    * @param process A function what is supposed to be called on new files.
    */
  def watch(process: (Seq[String]) => Unit): Unit
}
