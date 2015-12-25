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
package com.snowplowanalytics.sauna
package processors

// java
import java.io.InputStream

/**
 * After new file appeared, Sauna should process it somehow.
 * That's what a Processor does.
 */
trait Processor {
  /**
   * Method that describes "how to process new files".
   *
   * File can be outside of local fs, e.g. on AWS S3, and
   * some important parameters might be encoded as part of 'filePath',
   * therefore this method has both 'filePath: String' and 'InputStream'
   *
   * @param fileName Full name of file.
   * @param is InputStream from it.
   */
  def process(fileName: String, is: InputStream): Unit
}