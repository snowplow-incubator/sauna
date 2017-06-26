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
package sendgrid

// java
import java.io.ByteArrayInputStream

// scalatest
import org.scalatest._

// akka
import akka.actor.{ActorSystem, ActorRef}

// play
import play.api.libs.json._

// sauna
import apis.OpsGenie
import observers.Observer._
import responders.opsgenie.CreateAlertResponder._


object CreateAlertResponderTest {

}

class CreateAlertResponderTest extends FunSuite with BeforeAndAfter {
  import CreateAlertResponderTest._
  import OpsGenie._

  implicit var system: ActorSystem = _

  before {
    system = ActorSystem("CreateAlertResponderTest")
  }

  after {
    system.terminate()
  }

}

