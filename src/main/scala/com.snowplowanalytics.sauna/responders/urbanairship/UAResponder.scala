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
    var appMapOld: AppMap = null
    for (rawLine <- fromInputStream(is).getLines) {
     getMapEntryForLine(appMapOld,rawLine) match {
         case Some(appToListMap) => { appMapOld = appToListMap }
         case _ => None
       }
    }
   urbanairship.maptoRequest(appMapOld)
  }

  /**
   * grows the map and checks if count of operations is less than the urban airship allowed limit of 10 million and make request it is exceeded
   *
   * @param listToIdentifierMap updated map of listname to Object list
   * @param appKey the appKey needed to locate the list in the AppKey to ListMap
   *
   */

//  def makeRequest(appToListMap: appMap): appMap = {
//      println("in make req"+appToListMap)
//      urbanairship.maptoRequest(appToListMap)
//      null
//  }

  /**
   * gets Option[UAInput] object from getMapEntryForLine and Maps it to the getEntry function and maps to None if gets Nothing from getMapEntryForLine
   *
   * @param rawline a line from the TSV file
   * @param appToListMap the map of appKey to list map of values
   * @return Option[l(istToIdentifierMap,appKey)] returns an Option of the tuple if it gets a UAInput object else it return None
   */
  def getMapEntryForLine(appToListMap: AppMap,rawLine: String): Option[AppMap] = {
    getEntryFromLine(rawLine) match {
      case Some(uaInput) => Some(updateAppMap(appToListMap,uaInput))
      case _ => None
    }
  }
  
  


  def updateListMap(listToAirshipMap: ListMap, uaInput:UAInput): ListMap = { 
    listToAirshipMap + (uaInput.listName -> (listToAirshipMap(uaInput.listName) :+ new Airship(uaInput.idType,uaInput.id)))
  }

  def insertToAppMap(appToListMap: AppMap, listToAirshipMap:ListMap,appKey:String): AppMap = { 
    appToListMap + (appKey -> listToAirshipMap) 
  }
  
  def insertToListMap(listToAirshipMap: ListMap,listName: String,airship: Airship): ListMap = { 
    listToAirshipMap + (listName -> List(airship)) 
  }
  
  def getNewAppMap(appKey:String,listToAirshipMap:ListMap): AppMap = {
    Map(appKey -> listToAirshipMap)
  }
  
  def getNewListMap(listName:String,airship:Airship):ListMap = { 
    Map(listName -> List(airship))
    }
  
  /**
   * makes an updated appToListMap including the values from the input UAInput Object
   *
   * @param uaInput the UAInput object for a line in TSV
   * @param appToListMap the map of appKey to list map of values
   * @return updated appToListMap including the UAInput object
   */
  def updateAppMap(appToListMap: AppMap,uaInput: UAInput): (AppMap) = {
    val appTolistMapNew: AppMap = if (appToListMap == null) {
     getNewAppMap(uaInput.appKey,getNewListMap(uaInput.listName,new Airship(uaInput.idType, uaInput.id)))
    }
    else if(!appToListMap.contains(uaInput.appKey))
    {
      insertToAppMap(appToListMap,  getNewListMap(uaInput.listName,new Airship(uaInput.idType, uaInput.id)),uaInput.appKey)
    }
    else {
      val listMapForApp: ListMap = appToListMap(uaInput.appKey)
      val listToMap: ListMap = if (listMapForApp.contains(uaInput.listName)) {
        listMapForApp ++ updateListMap(listMapForApp,uaInput) 
      } else {
        insertToListMap(listMapForApp,uaInput.listName,new Airship(uaInput.idType, uaInput.id))
      }
      appToListMap + (uaInput.appKey -> listToMap)
    }
    appTolistMapNew
  }
}

object UAResponder {

  case class Airship(identifierType: String, identifier: String)

  case class UAInput(appKey: String, listName: String, idType: String, id: String)
  
  type ListMap = Map[String, List[Airship]]
  
  type AppMap = Map[String, Map[String, List[Airship]]]
  val AppMap = Map
  
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


