package com.snowplowanalytics.sauna

// java
import java.io.File

import com.snowplowanalytics.sauna.config.SaunaConfig

// scalatest
import org.scalatest._

class SaunaConfigTest extends FunSuite {
  test("config load from file") {
    val fileName = "src/test/resources/test.conf"
    val exceptedConfig = SaunaConfig("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k")
    val config = SaunaConfig(new File(fileName))

    assert(exceptedConfig === config)
  }
}