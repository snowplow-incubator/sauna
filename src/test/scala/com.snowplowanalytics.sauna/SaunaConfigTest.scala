package com.snowplowanalytics.sauna

// java
import java.io.File

// scalatest
import org.scalatest._

class SaunaConfigTest extends FunSuite {
  test("config load from file") {
    val fileName = "src/test/resources/test.conf"
    val exceptedConfig = SaunaConfig("a", "b", "c", "h", "d", "i", "e", "f", "g")
    val config = SaunaConfig(new File(fileName))

    assert(exceptedConfig === config)
  }
}