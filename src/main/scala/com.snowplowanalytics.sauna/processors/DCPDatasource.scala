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
package com.snowplowanalytics.sauna.processors

import java.io.InputStream

import com.snowplowanalytics.sauna.loggers.Logger

/**
  * Does stuff for Optimizely Dynamic Customer Profiles feature.
  */
class DCPDatasource extends Processor { self: Logger =>
  import DCPDatasource._

  override def process(fileName: String, is: InputStream): Unit = fileName match {
    case pathRegexp(service, datasource, _attrs) =>
      val attrs = _attrs.split(",")

      if (attrs.isEmpty) {
        self.notification("Should be at least one attribute.")
        return
      }

      if (!attrs.contains("customerId")) {
        self.notification("Attribute 'customerId' must be included.")
        return
      }

      println(s"service = $service, datasource = $datasource, attrs = ${attrs.mkString(" ")}")

    case _ =>
}

object DCPDatasource {
  val pathRegexp =
    """.*com\.optimizely/
      |dcp_datasource/
      |v1/
      |(.*?)/
      |(.*?)/
      |tsv:([^\/]+)/
      |.*$
    """.stripMargin
      .replaceAll("[\n ]", "")
      .r
  }
}
