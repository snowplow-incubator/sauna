package com.snowplowanalytics.sauna
package responders
package urbanairship

import scala.io.Source.fromInputStream
import java.io.DataOutputStream;
import org.apache.commons.io.IOUtils;
import java.io.File;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.net.URL;
import java.util.Map.Entry;
import scala.io.Source
import scala.collection;
import scala.collection.mutable.{ Map => MutableMap }
import javax.net.ssl.HttpsURLConnection;
import java.util.zip.GZIPOutputStream
import gvjava.org.json.JSONObject
import apis.UrbanAirship
import loggers.Logger.Notification
import akka.actor.{ActorRef, Props}
import responders.Responder.FileAppeared
import responders.urbanairship.UAResponder.Airship
import scala.concurrent.ExecutionContext.Implicits.global



class UAResponder(urbanairship: UrbanAirship)
                (implicit logger: ActorRef) extends Responder {

  import UAResponder._
  
 val pathPattern =
    """.*com\.urbanairship/
      |static_lists/
      |v1/
      |tsv:\*/
      |.+$
    """.stripMargin
       .replaceAll("[\n ]", "")
       
  
 override def process(fileAppeared: FileAppeared): Unit = {
    import fileAppeared._
   convertTSV(is)
  }
   
  def convertTSV(is: InputStream):Unit = {
      
    
    var appToListMap = Map[String, Map[String, List[Airship]]]()

    var listToIdentifierMap = Map[String, List[Airship]]()
    
    var count=0

    for (line <- fromInputStream(is).getLines()) {

      val rawlines = line.split("\t",-1)

      val lines = for (i <- rawlines) yield i.substring(1, i.length() - 1)
      
      

      if (!appToListMap.contains(lines(0))) {
      
        listToIdentifierMap = Map[String, List[Airship]](lines(1) -> List(new Airship(lines(2), lines(3))))
      } else {
        
        if (listToIdentifierMap.contains(lines(1))) {
          listToIdentifierMap = appToListMap(lines(0))
          
          listToIdentifierMap += (lines(1) -> (listToIdentifierMap(lines(1)):+new Airship(lines(2), lines(3))))
        } else {
          
          listToIdentifierMap += (lines(1) -> List(new Airship(lines(2), lines(3))))
        }
      }

      appToListMap += (lines(0) -> listToIdentifierMap)
      count+=1
      
      if(count>=10000000)
      {
        urbanairship.MaptoRequest(appToListMap)
        appToListMap=Map()
        count=0
      }
        
      
    }
    
    if(appToListMap.size>0)
      urbanairship.MaptoRequest(appToListMap)
    

  }
  

}

object UAResponder  {
 
  
  
  case class Airship(identifierType: String, identifier: String)
  
  def apply(urbanairship: UrbanAirship)(implicit logger: ActorRef): Props =
    Props(new UAResponder(urbanairship))
  
}


