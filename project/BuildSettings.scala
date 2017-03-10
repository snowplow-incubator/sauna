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

// SBT
import sbt._
import Keys._

// sbt-assembly
import sbtassembly.AssemblyPlugin.autoImport._
import sbtassembly.AssemblyPlugin.defaultShellScript

// avroghugger
import sbtavrohugger.SbtAvrohugger._

// bintray-sbt
import bintray.{BintrayIvyResolver, BintrayRepo, BintrayCredentials}
import bintray.BintrayPlugin._
import bintray.BintrayKeys._

// sbt-native-packager
import com.typesafe.sbt.packager.universal.UniversalPlugin.autoImport._
import com.typesafe.sbt.packager.archetypes.JavaAppPackaging.autoImport._
import com.typesafe.sbt.packager.SettingsHelper._

object BuildSettings {

  lazy val buildSettings = Seq[Setting[_]](
    organization  := "com.snowplowanalytics",
    scalaVersion  := "2.11.8",
    scalacOptions := Seq(
      "-deprecation",
      "-encoding", "UTF-8",
      "-feature",
      "-unchecked",
      "-Ywarn-dead-code",
      "-Ywarn-inaccessible",
      "-Ywarn-infer-any",
      "-Ywarn-nullary-override",
      "-Ywarn-nullary-unit",
      "-Ywarn-numeric-widen",
      "-Ywarn-unused",
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

    parallelExecution in Test := false // possible race bugs
  )

  // Assembly settings
  lazy val sbtAssemblySettings: Seq[Setting[_]] = Seq(

    // Executable jarfile
    assemblyOption in assembly ~= { _.copy(prependShellScript = Some(defaultShellScript)) },

    // Name it as an executable
    assemblyJarName in assembly := { s"${name.value}" },

    // Make this executable
    mainClass in assembly := Some("com.snowplowanalytics.sauna.Sauna"),

    test in assembly := {},        // Speed up packaging

    assemblyMergeStrategy in assembly := {
      case PathList("org", "apache", "commons", "logging", xs @ _*) =>
        // By default, sbt-assembly checks class files with same relative paths for 100% identity
        // However, sometimes different dependency-of-dependency might have different library version, e.g. content
        // so, we should chose one of conflicting versions
        MergeStrategy.first

      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }
  )

  // Bintray publish settings
  lazy val publishSettings = bintraySettings ++ Seq[Setting[_]](
    licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0.html")),
    bintrayOrganization := Some("snowplow"),
    bintrayRepository := "snowplow-generic",
    publishMavenStyle := false,

    // Custom Bintray resolver used to publish package with custom Ivy patterns (custom path in Bintray)
    // This fragile piece of code should be borrowed very carefully
    publishTo in bintray := {
      for {
        bintrayOrg    <- bintrayOrganization.value
        credentials   <- BintrayCredentials.read(bintrayCredentialsFile.value).right.toOption.flatten
        bintrayRepo    = BintrayRepo(credentials, Some(bintrayOrg), name.value)
        connectedRepo  = bintrayRepo.client.repo(bintrayOrg, bintrayRepository.value)
        bintrayPackage = connectedRepo.get(name.value)
        ivyResolver    = BintrayIvyResolver(
          bintrayRepository.value,
          bintrayPackage.version(version.value),
          Seq(s"${name.value}_${version.value.replace('-', '_')}.[ext]"), // Ivy Pattern
          release = true)
      } yield new RawRepository(ivyResolver)
    }
  )

  // Add access to build info in code
  lazy val scalifySettings = Seq(

    (scalaSource in avroConfig) := (sourceManaged in Compile).value / "avro",

    sourceGenerators in Compile += Def.task {
      val file = (sourceManaged in Compile).value / "generated" / "settings.scala"
      IO.write(file, s"""package com.snowplowanalytics.${name.value}.generated
                         |object ProjectSettings {
                         |  val version = "${version.value}"
                         |  val name = "${name.value}"
                         |  val organization = "${organization.value}"
                         |  val scalaVersion = "${scalaVersion.value}"
                         |}
                         |""".stripMargin)
      Seq(file)
    }.taskValue
  )

  // Packaging (sbt-native-packager) settings
  lazy val deploySettings = Seq(
    // Don't publish MD5/SHA checksums
    checksums := Nil,

    // Assemble zip archive with fat jar
    mappings in Universal := {
      // Use fat jar built by sbt-assembly
      val fatJar = (assembly in Compile).value

      // We don't need anything except fat jar
      val nativePackagerFiles = Nil

      // Add the fat jar
      nativePackagerFiles :+ (fatJar -> ("/" + fatJar.getName))
    },

    scriptClasspath := Seq((assemblyJarName in assembly).value)

  ) ++ makeDeploymentSettings(Universal, packageBin in Universal, "zip")

}
