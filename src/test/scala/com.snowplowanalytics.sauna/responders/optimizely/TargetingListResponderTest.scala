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
package optimizely

// scalatest
import org.scalatest._

import play.api.libs.json.Json

// sauna
import apis.Optimizely._

class TargetingListResponderTest extends FunSuite {

  test("unapply invalid input") {
    val s = "qwerty"

    assert(TargetingListResponder.extract(s) === None)
  }

  test("unapply valid input") {
    val s = "4034532101\tdec_ab_group\tDecember 2015 A/B group\t1\tlogin,signup\t38071d80-1f03-4c50-ba94-f3daafd5a8c0"

    assert(TargetingListResponder.extract(s) === Some(TargetingListLine("4034532101", "dec_ab_group", "December 2015 A/B group", 1, Some("login,signup"), "38071d80-1f03-4c50-ba94-f3daafd5a8c0")))
  }

  test("unapply empty keyFields") {
    val s = "4034532101\tdec_ab_group\tDecember 2015 A/B group\t1\t\t38071d80-1f03-4c50-ba94-f3daafd5a8c0"

    assert(TargetingListResponder.extract(s) === Some(TargetingListLine("4034532101", "dec_ab_group", "December 2015 A/B group", 1, None, "38071d80-1f03-4c50-ba94-f3daafd5a8c0")))
  }

  test("merge empty") {
    val tls = List.empty

    val _ = intercept[NoSuchElementException] {
      val _ = merge(tls)
    }
  }

  test("merge one valid") {
    val tl = TargetingListLine("4034532101", "dec_ab_group", "December 2015 A/B group", 1, Some("login,signup"), "38071d80-1f03-4c50-ba94-f3daafd5a8c0")

    assert(merge(List(tl)) === Json.parse("{\"name\":\"dec_ab_group\", \"description\":\"December 2015 A/B group\", \"list_type\":1, \"key_fields\":\"login,signup\", \"list_content\":\"38071d80-1f03-4c50-ba94-f3daafd5a8c0\",\"format\":\"tsv\"}"))
  }

  test("merge one empty keyFields") {
    val tl = TargetingListLine("4034532101", "dec_ab_group", "December 2015 A/B group", 1, None, "38071d80-1f03-4c50-ba94-f3daafd5a8c0")

    assert(merge(List(tl)) === Json.parse("{\"name\":\"dec_ab_group\", \"description\":\"December 2015 A/B group\", \"list_type\":1, \"key_fields\":null, \"list_content\":\"38071d80-1f03-4c50-ba94-f3daafd5a8c0\",\"format\":\"tsv\"}"))
  }

  test("merge several") {
    val tl1 = TargetingListLine("4034532101", "dec_ab_group", "December 2015 A/B group", 1, Some("login,signup"), "38071d80-1f03-4c50-ba94-f3daafd5a8c0")
    val tl2 = TargetingListLine("4034532101", "dec_ab_group", "December 2015 A/B group", 1, Some("login,signup"), "76545674-1f03-4c50-ba94-f3daafd5a8c0")

    assert(merge(List(tl1, tl2)) === Json.parse("{\"name\":\"dec_ab_group\", \"description\":\"December 2015 A/B group\", \"list_type\":1, \"key_fields\":\"login,signup\", \"list_content\":\"38071d80-1f03-4c50-ba94-f3daafd5a8c0,76545674-1f03-4c50-ba94-f3daafd5a8c0\",\"format\":\"tsv\"}"))
  }
}