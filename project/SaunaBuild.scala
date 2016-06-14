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
import sbt._
import Keys._
import sbtassembly.AssemblyKeys._
import sbtassembly.{PathList, MergeStrategy}

object SaunaBuild extends Build {
  val playVersion = "2.4.4"
  val akkaVersion = "2.4.1"

  lazy val project = Project("sauna", file("."))
    .settings(buildSettings: _*)
    .settings(
      libraryDependencies ++= Seq(
        "com.github.tototoshi" %% "scala-csv" % "1.2.2",
        "com.github.seratch" %% "awscala" % "0.5.+",
        "com.typesafe" % "config" % "1.3.0",
        "com.typesafe.play" %% "play-json" % playVersion,
        "com.typesafe.play" %% "play-ws" % playVersion,
        "com.typesafe.akka" %% "akka-actor" % akkaVersion,
        "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
        "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
        "com.typesafe.akka" %% "akka-cluster-tools" % akkaVersion,
        "org.json" % "json" % "20160212",
        "commons-io" % "commons-io" % "2.5",
        
        "org.scalatest" %% "scalatest" % "2.2.4" % "test"
      )
    )

  lazy val buildSettings = Seq[Setting[_]](
    organization  := "com.snowplowanalytics",
    name          := "sauna",
    version       := "0.1.0",
    description   := "A decisioning and response framework",
    scalaVersion  := "2.11.7",
    scalacOptions := Seq(
                       "-deprecation",
                       "-encoding", "UTF-8",
                       "-feature",
                       "-unchecked",
                       "-Xfatal-warnings",
                       "-Ywarn-dead-code",
                       "-Ywarn-inaccessible",
                       "-Ywarn-infer-any",
                       "-Ywarn-nullary-override",
                       "-Ywarn-nullary-unit",
                       "-Ywarn-numeric-widen",
                       "-Ywarn-unused",
                       "-Ywarn-unused-import",
                       "-Ywarn-value-discard"
                     ),
    javacOptions := Seq(
                      "-source", "1.8",
                      "-target", "1.8",
                      "-Xlint"
                    ),

    scalacOptions in (Compile, console) ~= (_ filterNot (_ == "-Xfatal-warnings")),
    scalacOptions in (Test, console) := (scalacOptions in (Compile, console)).value,

    // force scala version
    // http://stackoverflow.com/questions/27809280/suppress-sbt-eviction-warnings
    ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) },

    parallelExecution in Test := false, // possible race bugs

    test in assembly := {}, // speed up packaging
    assemblyMergeStrategy in assembly := {
      case PathList("org", "apache", "commons", "logging", xs @ _*) =>
        // by default, sbt-assembly checks class files with same relative paths for 100% identity
        // however, sometimes different dependency-of-dependency might have different library version, e.g. content
        // so, we should chose one of conflicting versions
        MergeStrategy.first

      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }
  )
}