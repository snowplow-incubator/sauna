package com.snowplowanalytics.sauna
package responders
package urbanairship

import scala.io.Source.fromInputStream

import java.io.InputStream

import apis.urbanAirship

import akka.actor.{ ActorRef, Props }

import responders.Responder.FileAppeared

  /**
 * Does stuff for UrbanAirship Static List upload feature.
 *
 * @see https://github.com/snowplow/sauna/wiki/Urban-Airship-Responder-user-guide
 * @param urbanairship Instance of UrbanAirship.
 * @param logger A logger actor.
 */

class UAResponder(urbanairship: urbanAirship)(implicit logger: ActorRef) extends Responder {

  import UAResponder._

  val pathPattern =
    """.*com\.urbanairship/
      |static_lists/
      |v1/
      |tsv:\*/
      |.+$
    """.stripMargin
      .replaceAll("[\n ]", "")

  def process(fileAppeared: FileAppeared): Unit = {
    import fileAppeared._
    convertTSV(is)
  }
  
/**
   * Converts the TSV data and groups it in a map so as to make batched upload requests to UrbanAirship
   *
   * @param is the InputStream of the TSV file 
   * @return a map of the data grouped by application keys and then by listNames
   */

  def convertTSV(is: InputStream): Unit = {

    var appToListMap = Map[String, Map[String, List[Airship]]]()

    var listToIdentifierMap = Map[String, List[Airship]]()

    var count = 0

    fromInputStream(is).getLines.foreach { rawLine =>

      val line = rawLine.replace("\"", "").trim

      val Array(appKey, listName, idType, id) = line.split("\t", -1)

      if (!appToListMap.contains(appKey)) {

        listToIdentifierMap = Map[String, List[Airship]](listName -> List(new Airship(idType, id)))
      } else {

        if (listToIdentifierMap.contains(listName)) {
          listToIdentifierMap = appToListMap(appKey)

          listToIdentifierMap += (listName -> (listToIdentifierMap(listName) :+ new Airship(idType, id)))
        } else {

          listToIdentifierMap += (listName -> List(new Airship(idType, id)))
        }
      }

      appToListMap += (appKey -> listToIdentifierMap)
      count += 1

      if (count >= 10000000) {
        urbanairship.maptoRequest(appToListMap)
        appToListMap = Map()
        count = 0
      }
    }

    if (appToListMap.size > 0)
      urbanairship.maptoRequest(appToListMap)

  }

}

object UAResponder {

  case class Airship(identifierType: String, identifier: String)

  def apply(urbanairship: urbanAirship)(implicit logger: ActorRef): Props =
    Props(new UAResponder(urbanairship))

}


