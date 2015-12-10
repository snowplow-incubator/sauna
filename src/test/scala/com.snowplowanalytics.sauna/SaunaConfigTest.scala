package com.snowplowanalytics.sauna

import java.io.File

import org.scalatest._

class SaunaConfigTest extends FunSuite {
  test("config load from file") {
    val fileName = "src/test/resources/test.conf"
    val exceptedConfig = SaunaConfig("a", "b", "c", "d", "e")
    val config = SaunaConfig(new File(fileName))

    assert(exceptedConfig === config)
  }
}
