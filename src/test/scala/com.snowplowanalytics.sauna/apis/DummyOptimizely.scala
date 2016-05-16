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
package apis

// scala
import scala.concurrent.Future

// play
import play.api.libs.ws.WSResponse

// sauna
import apis.Optimizely.TargetingListLine


class DummyOptimizely extends Optimizely("", null) { // DummyOptimizely wont need these value
  override def postTargetingLists(tlData: List[TargetingListLine]): Future[WSResponse] =
    Future.failed(new Exception("Using DummyOptimizely"))

  override def getOptimizelyS3Credentials(dcpDatasourceId: String): Future[Option[(String, String)]] =
    Future.failed(new Exception("Using DummyOptimizely"))
}