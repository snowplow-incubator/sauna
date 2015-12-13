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

object SaunaBuild extends Build {
  val playVersion = "2.4.4"

  lazy val project = Project("sauna", file("."))
    .settings(buildSettings: _*)
    .settings(
      libraryDependencies ++= Seq(
        "com.github.seratch" %% "awscala" % "0.5.+",
        "com.typesafe" % "config" % "1.3.0",
        "com.typesafe.play" %% "play-json" % playVersion,
        "com.typesafe.play" %% "play-ws" % playVersion,

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
    ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }
  )
}