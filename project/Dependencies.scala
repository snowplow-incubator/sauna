/*
 * Copyright (c) 2016-2017 Snowplow Analytics Ltd. All rights reserved.
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

import sbt._

object Dependencies {
  object V {
    val typesafeConfig    = "1.3.0"
    val totoshi           = "1.2.2"
    val awscala           = "0.5.7"
    val gfcAwsKinesis     = "0.10.2"
    val gfcAwsKinesisAkka = "0.10.2"
    val nscalaTime        = "2.12.0"
    val scopt             = "3.5.0"
    val avro4s            = "1.5.4"
    val play              = "2.4.8"
    val akka              = "2.4.9"
    val igluCore          = "0.1.0"
    val igluScalaClient   = "0.5.0"
    val scalaTest         = "2.2.4"
    val opsGenie          = "2.8.1"
  }

  object Libraries {
    // Java
    val typesafeConfig    = "com.typesafe"           % "config"                % V.typesafeConfig

    // Scala
    val totoshi           = "com.github.tototoshi"   %% "scala-csv"            % V.totoshi
    val awscala           = "com.github.seratch"     %% "awscala"              % V.awscala
    val gfcAwsKinesis     = "com.gilt"               %% "gfc-aws-kinesis"      % V.gfcAwsKinesis
    val gfcAwsKinesisAkka = "com.gilt"               %% "gfc-aws-kinesis-akka" % V.gfcAwsKinesisAkka
    val nscalaTime        = "com.github.nscala-time" %% "nscala-time"          % V.nscalaTime
    val scopt             = "com.github.scopt"       %% "scopt"                % V.scopt
    val avro4s            = "com.sksamuel.avro4s"    %% "avro4s-core"          % V.avro4s
    val playJson          = "com.typesafe.play"      %% "play-json"            % V.play
    val playWs            = "com.typesafe.play"      %% "play-ws"              % V.play
    val akkaActor         = "com.typesafe.akka"      %% "akka-actor"           % V.akka
    val akkaStreams       = "com.typesafe.akka"      %% "akka-stream"          % V.akka
    val akkaCluster       = "com.typesafe.akka"      %% "akka-cluster"         % V.akka
    val akkaClusterTools  = "com.typesafe.akka"      %% "akka-cluster-tools"   % V.akka
    val igluCore          = "com.snowplowanalytics"  %% "iglu-core"            % V.igluCore
    val igluScalaClient   = "com.snowplowanalytics"  %% "iglu-scala-client"    % V.igluScalaClient
    val opsGenieSDK       = "com.opsgenie.integration" % "sdk-shaded"                % V.opsGenie

    // Test
    val akkaTestkit       = "com.typesafe.akka"      %% "akka-testkit"         % V.akka              // scope to test?
    val scalaTest         = "org.scalatest"          %% "scalatest"            % V.scalaTest         % "test"
  }
}
