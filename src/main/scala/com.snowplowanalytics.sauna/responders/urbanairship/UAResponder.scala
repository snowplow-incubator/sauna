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

  var appToListMap = Map[String, Map[String, List[Airship]]]()
  var count = 0
  
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
   * Converts the TSV data and groups it in a map so as to make batched upload requests to UrbanAirship
   *
   * @param is the InputStream of the TSV file 
   * @return a map of the data grouped by application keys and then by listNames
   */
  
  def getLinesFromTSV(is:InputStream):Unit={
    
    for(rawLine <- fromInputStream(is).getLines)
    {
      getMapEntryForLine(rawLine,appToListMap) match{
       case Right(msg) =>
       case Left(test) => Left(makeRequest(test._1,test._2))  
     }
    }
    if (appToListMap.size > 0)
      urbanairship.maptoRequest(appToListMap)
  }
  
  def makeRequest(listToIdentifierMap:Map[String, List[Airship]],appKey:String):Unit={
     if(appKey!=null)
     {
       appToListMap += (appKey -> listToIdentifierMap)
        count += 1
        if (count >= 10000000) {
          urbanairship.maptoRequest(appToListMap)
          appToListMap = Map()
          count = 0
         }
      }
  }
  

  
  def getMapEntryForLine(rawLine:String,appToListMap:Map[String, Map[String, List[Airship]]]):Either[(Map[String, List[Airship]],String),Unit] = {
      
    getEntryFromLine(rawLine) match {
  case Right(msg) => Right(logger ! Notification(msg))
  case Left(uaInput) => Left(getEntry(uaInput,appToListMap))
    }
  }
  
  def getEntry(uaInput:UAInput,appToListMap:Map[String, Map[String, List[Airship]]]):(Map[String, List[Airship]],String)={
    val listToIdentifierMap:Map[String, List[Airship]] = if (!appToListMap.contains(uaInput.appKey.get)) {
         Map[String, List[Airship]](uaInput.listName.get -> List(new Airship(uaInput.idType.get, uaInput.id.get)))
      } else {
        val listMapForApp:Map[String, List[Airship]] = appToListMap(uaInput.appKey.get)
        val listMap:Map[String, List[Airship]] = if (listMapForApp.contains(uaInput.listName.get)) {
        val newListEntry:Map[String, List[Airship]]= Map(uaInput.listName.get -> (listMapForApp(uaInput.listName.get) :+ new Airship(uaInput.idType.get, uaInput.id.get)))  
           listMapForApp ++ newListEntry
        } else {
          val newListEntry:Map[String, List[Airship]]= Map(uaInput.listName.get -> (List[Airship]() :+ new Airship(uaInput.idType.get, uaInput.id.get)))
            listMapForApp ++ newListEntry 
        }
        listMap
      }    
      (listToIdentifierMap,uaInput.appKey.get)
  }
  
  def getEntryFromLine(rawLine:String):Either[UAInput,String]={
    
    val line = rawLine.replace("\"", "").trim
      
     line.split("\t", -1) match 
     { 
        case Array(appKey, listName,idType , id) => Left( UAInput(Some(appKey),Some(listName),Some(idType),Some(id)))
        case _ => Right("not enough fields") 
     }
  }
  

}

object UAResponder {

  case class Airship(identifierType: String, identifier: String)

  case class UAInput(appKey:Option[String],listName:Option[String],idType: Option[String], id: Option[String])
  
  def apply(urbanairship: urbanAirship)(implicit logger: ActorRef): Props =
    Props(new UAResponder(urbanairship))

}


