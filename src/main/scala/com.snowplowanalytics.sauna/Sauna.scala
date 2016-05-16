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

// java
import akka.cluster.Cluster

// typesafe-config
import com.typesafe.config.ConfigFactory

// akka
import akka.actor._
import akka.cluster.singleton._

// sauna
import actors._
import config._

/**
 * Main class, starts the Sauna program.
 */
object Sauna extends App {
  if (args.length == 0) {
    println("""Usage: 'sbt "run <path_to_file_with_credentials>" <port>' """)
    System.exit(1)
  }

  // configuration
  // this very ineffective search will be 1) executed once 2) executed on list with length <= 10
  // so readability here is more important, than performance
  val respondersLocation = args.zipWithIndex
                               .find { case (_, r) => args(r) == "--responders" }
                               .map { case (_, r) => args(r + 1) }
                               .getOrElse(throw new RuntimeException("No location for responders defined!"))
  val respondersConfig = RespondersConfig(respondersLocation)

  val observersLocation = args.zipWithIndex
                              .find { case (_, r) => args(r) == "--observers" }
                              .map { case (_, r) => args(r + 1) }
                              .getOrElse(throw new RuntimeException("No location for observers defined!"))
  val observersConfig = ObserversConfig(observersLocation)

  val loggersLocation = args.zipWithIndex
                            .find { case (_, r) => args(r) == "--loggers" }
                            .map { case (_, r) => args(r + 1) }
                            .getOrElse(throw new RuntimeException("No location for loggers defined!"))
  val loggersConfig = LoggersConfig(observersLocation)

  val port = args.last
  val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$port")
                            .withFallback(ConfigFactory.load())
  val system = ActorSystem("sauna", config)
  val cluster = Cluster(system)

  if (observersConfig.localObserverEnabled) {
    // this actor runs on all nodes
    system.actorOf(Props(new UbiquitousActor(respondersConfig, observersConfig, loggersConfig)), "UbiquitousActor")
  }

  if (observersConfig.s3ObserverEnabled) {
    // this actor runs on one and only one node
    system.actorOf(
      ClusterSingletonManager.props(
        singletonProps = Props(new SingletonActor(respondersConfig, observersConfig, loggersConfig)),
        terminationMessage = PoisonPill,
        settings = ClusterSingletonManagerSettings(system)
      ),
      name = "SingletonActor"
    )
  }
}