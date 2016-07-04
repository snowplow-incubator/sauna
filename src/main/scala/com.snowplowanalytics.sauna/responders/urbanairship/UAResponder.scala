package com.snowplowanalytics.sauna
package responders
package urbanairship

import scala.io.Source.fromInputStream

import loggers.Logger.Notification

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

    var appToListMap = Map[String, Map[String, List[Airship]]]()
    var count = 0
      
  def process(fileAppeared: FileAppeared): Unit = {
    import fileAppeared._
    getLinesFromTSV(is)
  }

  /**
   * Converts the TSV data and groups it in a map and does batched upload requests to UrbanAirship
   *
   * @param is the InputStream of the TSV file
   *
   */

  def getLinesFromTSV(is: InputStream): Unit = {
    
  appToListMap = Map[String, Map[String, List[Airship]]]()   
  count = 0
  
    for (rawLine <- fromInputStream(is).getLines) {
      getMapEntryForLine(rawLine) match {
        case Some(test) => makeRequest(test._1, test._2)
        case _ => logger ! Notification("not enough fields in TSV!")
      }
    }
    if (appToListMap.size > 0)
      urbanairship.maptoRequest(appToListMap)
  }

  /**
   * grows the map and checks if count of operations is less than the urban airship allowed limit of 10 million and make request it is exceeded
   *
   * @param listToIdentifierMap updated map of listname to Object list
   * @param appKey the appKey needed to locate the list in the AppKey to ListMap
   *
   */

  def makeRequest(listToIdentifierMap: Map[String, List[Airship]], appKey: String): Unit = {
    appToListMap += (appKey -> listToIdentifierMap)
    count += 1
    if (count >= 10000000) {
      urbanairship.maptoRequest(appToListMap)
      appToListMap = Map[String, Map[String, List[Airship]]]()
      count = 0
    }
  }

  /**
   * gets Option[UAInput] object from getMapEntryForLine and Maps it to the getEntry function and maps to None if gets Nothing from getMapEntryForLine
   *
   * @param rawline a line from the TSV file
   * @param appToListMap the map of appKey to list map of values
   * @return Option[l(istToIdentifierMap,appKey)] returns an Option of the tuple if it gets a UAInput object else it return None
   */
  def getMapEntryForLine(rawLine: String): Option[(Map[String, List[Airship]], String)] = {

    getEntryFromLine(rawLine) match {
      case Some(uaInput) => Some(getEntry(uaInput))
      case _ => None
    }
  }

  /**
   * makes an updated listToidentifierMap including the values from the input UAInput Object
   *
   * @param uaInput the UAInput object for a line in TSV
   * @param appToListMap the map of appKey to list map of values
   * @return (listToIdentifierMap,uaInput.appKey) the tuple of updated listToIdentfierMap including the new UAInput object and the appKey for that map
   */
  def getEntry(uaInput: UAInput): (Map[String, List[Airship]], String) = {
    val listToIdentifierMap: Map[String, List[Airship]] = if (!appToListMap.contains(uaInput.appKey)) {
      Map[String, List[Airship]](uaInput.listName -> List(new Airship(uaInput.idType, uaInput.id)))
    } else {
      val listMapForApp: Map[String, List[Airship]] = appToListMap(uaInput.appKey)
      val listMap: Map[String, List[Airship]] = if (listMapForApp.contains(uaInput.listName)) {
        val newListEntry: Map[String, List[Airship]] = Map(uaInput.listName -> (listMapForApp(uaInput.listName) :+ new Airship(uaInput.idType, uaInput.id)))
        listMapForApp ++ newListEntry
      } else {
        val newListEntry: Map[String, List[Airship]] = Map(uaInput.listName -> (List[Airship]() :+ new Airship(uaInput.idType, uaInput.id)))
        listMapForApp ++ newListEntry
      }
      listMap
    }
    (listToIdentifierMap, uaInput.appKey)
  }

}

object UAResponder {

  case class Airship(identifierType: String, identifier: String)

  case class UAInput(appKey: String, listName: String, idType: String, id: String)

  def apply(urbanairship: urbanAirship)(implicit logger: ActorRef): Props =
    Props(new UAResponder(urbanairship))

  /**
   * extracts the keys from a TSV line and returns it in the form of an Object
   *
   * @param rawLine a TSV line from the file
   * @return Option[UAInput] returns a UAInput object if the line is in proper format with all 4 keys or else returns None
   *
   */

  def getEntryFromLine(rawLine: String): Option[UAInput] = {
    val line = rawLine.replace("\"", "").trim

    line.split("\t", -1) match {
      case Array(appKey, listName, idType, id) => Some(UAInput(appKey, listName, idType, id))
      case _ => None
    }
  }
}


