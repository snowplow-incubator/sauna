package com.snowplowanalytics.sauna

// play
import play.api.libs.ws.WSClient
import play.api.libs.ws.ning.NingWSClient

// scala-csv
import com.github.tototoshi.csv.TSVFormat

/**
 * Shares common variables in single instance.
 */
package object utils {
  val tsvFormat = new TSVFormat {} // forces scala-csv to use tsv
  val wsClient: WSClient = NingWSClient()
}
