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

// akka
import akka.actor._

// sauna
import actors._

/**
 * Main class, starts the Sauna program.
 */
object Sauna extends App {

  SaunaOptions.parser.parse(args, SaunaOptions.initial) match {
    case Some(options) =>
      println("Parsed configs:")
      for (option <- options.extract.productIterator.toList)
        if (option != None && option != List())
          println(option)

      run(options.extract)
    case None =>
      sys.exit(1)
  }

  /**
   * Start observers with provided configuration
   *
   * @param saunaSettings options parsed from command line
   */
  def run(saunaSettings: SaunaSettings): Unit = {
    val system = ActorSystem("sauna")
    println("Actor system started...")
    system.actorOf(Props(new Mediator(saunaSettings)))
  }
}