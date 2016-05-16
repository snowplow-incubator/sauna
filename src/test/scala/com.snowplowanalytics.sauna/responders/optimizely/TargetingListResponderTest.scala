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

class TargetingListResponderTest extends FunSuite {
  test("unapply invalid input") {
    val s = "qwerty"

    assert(TargetingListResponder.extract(s) === None)
  }

  test("unapply valid input") {
    val s = "4034532101\tdec_ab_group\tDecember 2015 A/B group\t1\tlogin,signup\t38071d80-1f03-4c50-ba94-f3daafd5a8c0"

    assert(TargetingListResponder.extract(s) === Some(TargetingListResponder.Data("4034532101", "dec_ab_group", "December 2015 A/B group", 1, Some("login,signup"), "38071d80-1f03-4c50-ba94-f3daafd5a8c0")))
  }

  test("unapply empty keyFields") {
    val s = "4034532101\tdec_ab_group\tDecember 2015 A/B group\t1\t\t38071d80-1f03-4c50-ba94-f3daafd5a8c0"

    assert(TargetingListResponder.extract(s) === Some(TargetingListResponder.Data("4034532101", "dec_ab_group", "December 2015 A/B group", 1, None, "38071d80-1f03-4c50-ba94-f3daafd5a8c0")))
  }

  test("merge empty") {
    val tls = Seq()

    val _ = intercept[NoSuchElementException] {
      val _ = TargetingListResponder.merge(tls)
    }
  }

  test("merge one valid") {
    val tl = TargetingListResponder.Data("4034532101", "dec_ab_group", "December 2015 A/B group", 1, Some("login,signup"), "38071d80-1f03-4c50-ba94-f3daafd5a8c0")

    assert(TargetingListResponder.merge(Seq(tl)) === "{\"name\":\"dec_ab_group\", \"description\":\"December 2015 A/B group\", \"list_type\":1, \"key_fields\":\"login,signup\", \"list_content\":\"38071d80-1f03-4c50-ba94-f3daafd5a8c0\",\"format\":\"tsv\"}")
  }

  test("merge one empty keyFields") {
    val tl = TargetingListResponder.Data("4034532101", "dec_ab_group", "December 2015 A/B group", 1, None, "38071d80-1f03-4c50-ba94-f3daafd5a8c0")

    assert(TargetingListResponder.merge(Seq(tl)) === "{\"name\":\"dec_ab_group\", \"description\":\"December 2015 A/B group\", \"list_type\":1, \"key_fields\":null, \"list_content\":\"38071d80-1f03-4c50-ba94-f3daafd5a8c0\",\"format\":\"tsv\"}")
  }

  test("merge several") {
    val tl1 = TargetingListResponder.Data("4034532101", "dec_ab_group", "December 2015 A/B group", 1, Some("login,signup"), "38071d80-1f03-4c50-ba94-f3daafd5a8c0")
    val tl2 = TargetingListResponder.Data("4034532101", "dec_ab_group", "December 2015 A/B group", 1, Some("login,signup"), "76545674-1f03-4c50-ba94-f3daafd5a8c0")

    assert(TargetingListResponder.merge(Seq(tl1, tl2)) === "{\"name\":\"dec_ab_group\", \"description\":\"December 2015 A/B group\", \"list_type\":1, \"key_fields\":\"login,signup\", \"list_content\":\"38071d80-1f03-4c50-ba94-f3daafd5a8c0,76545674-1f03-4c50-ba94-f3daafd5a8c0\",\"format\":\"tsv\"}")
  }
}