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
    var appMapOld:appMap = null
    for (rawLine <- fromInputStream(is).getLines) {
     val appToMap:Option[appMap] = getMapEntryForLine(appMapOld,rawLine) match {
         case Some(appToListMap) => Some(appToListMap)
         case _ => None
       }
  
     if(appToMap.isDefined)
     {
       appMapOld = appToMap.get
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
  def getMapEntryForLine(appToListMap: appMap,rawLine: String): Option[appMap] = {

    getEntryFromLine(rawLine) match {
      case Some(uaInput) => Some(getEntry(appToListMap,uaInput))
      case _ => None
    }
  }
  
  
  def updateAppMap(appToListMap:appMap,listToAirshipMap:listMap,appKey:String):appMap={appToListMap.filterKeys(_ == appKey).transform((k,v)=>listToAirshipMap)}

  def updateListMap(listToAirshipMap:listMap,listName:String,airship:Airship):listMap={listToAirshipMap.filterKeys(_ == listName ).transform((k,v) => (v:+airship))}

  def insertToAppMap(appToListMap:appMap,listToAirshipMap:listMap,appKey:String):appMap={appToListMap + (appKey -> listToAirshipMap)}
  
  def insertToListMap(listToAirshipMap:listMap,listName:String,airship:Airship):listMap={listToAirshipMap + (listName -> List(airship))}
  
  def getNewAppMap(appKey:String,listToAirshipMap:listMap):appMap={appMap(appKey,listToAirshipMap)}
  
  def getNewListMap(listName:String,airship:Airship):listMap={listMap(listName , List(airship))}
  
  /**
   * makes an updated listToidentifierMap including the values from the input UAInput Object
   *
   * @param uaInput the UAInput object for a line in TSV
   * @param appToListMap the map of appKey to list map of values
   * @return (listToIdentifierMap,uaInput.appKey) the tuple of updated listToIdentfierMap including the new UAInput object and the appKey for that map
   */
  def getEntry(appToListMap:appMap,uaInput: UAInput): (appMap) = {
    val appTolistMapNew: appMap = if (appToListMap == null) {
     getNewAppMap(uaInput.appKey,getNewListMap(uaInput.listName,new Airship(uaInput.idType, uaInput.id)))
    }
    else if(!appToListMap.contains(uaInput.appKey))
    {
      insertToAppMap(appToListMap,  getNewListMap(uaInput.listName,new Airship(uaInput.idType, uaInput.id)),uaInput.appKey)
    }
    else {
      val listMapForApp: listMap = appToListMap(uaInput.appKey)
      val listToMap: listMap = if (listMapForApp.contains(uaInput.listName)) {
        listMapForApp ++ updateListMap(listMapForApp,uaInput.listName,new Airship(uaInput.idType, uaInput.id)) 
      } else {
        insertToListMap(listMapForApp,uaInput.listName,new Airship(uaInput.idType, uaInput.id))
      }
      appToListMap ++ updateAppMap(appToListMap,listToMap,uaInput.appKey)
    }
    appTolistMapNew
  }

}

object UAResponder {

  case class Airship(identifierType: String, identifier: String)

  case class UAInput(appKey: String, listName: String, idType: String, id: String)
  
  type listMap = Map[String, List[Airship]]
  def listMap(listName: String,list: List[Airship]) = Map(listName -> list)
  
  
  type appMap = Map[String, Map[String, List[Airship]]]
  def appMap(appKey: String,listToAirshipMap: listMap)= Map(appKey -> listToAirshipMap )
  
  
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


