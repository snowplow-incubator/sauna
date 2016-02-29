/*
 * Copyright (c) 2016 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.sauna
package observers

// java
import java.io.{FileInputStream, IOException}
import java.nio.file._
import java.nio.file.attribute._

// scala
import scala.collection.JavaConversions._
import scala.collection.mutable

// akka
import akka.actor.ActorRef
import akka.pattern.ask

// sauna
import loggers.Logger.Notification
import responders.Responder._

/**
 * Observes files in local filesystem.
 *
 * @param observedDir A directory what will be (recursively) watched for new files.
 * @param responders A Seq of ActorRef with underlying Responder. They will be called after new file appeared.
 * @param logger A logger actor.
 * @param self Actor in whose context observer runs.
 */
class LocalObserver(observedDir: String, responders: Seq[ActorRef], logger: ActorRef)
                   (implicit self: ActorRef) extends Observer {

  private def processEvent(event: WatchEvent.Kind[Path], path: Path): Unit = {
    if (event == StandardWatchEventKinds.ENTRY_CREATE) {
      val is = new FileInputStream(path.toFile)

      logger ! Notification(s"Detected new local file [$path].")

      responders.map { case responder =>
        responder ? FileAppeared(path.toString, is) // trigger responder
        
      }.foreach { case _ => // cleanup
        Files.deleteIfExists(path)
      }
    }
  }

  override def run(): Unit = {
    val watcher = new DirectoryWatcher(Paths.get(observedDir), processEvent)
    watcher.start()
  }
}

/**
 * Utility class, watches for all files in given directory, recursively.
 * @see http://download.oracle.com/javase/tutorial/essential/io/examples/WatchDir.java
 *
 * @param path A path to be watched.
 * @param processEvent How an event should be processed.
 * @param recursive Should watch for subdirectories or no.
 */
class DirectoryWatcher(path: Path,
                       processEvent: (WatchEvent.Kind[Path], Path) => Unit,
                       recursive: Boolean = true) {
  @volatile private var running = true
  private val watchService = FileSystems.getDefault
                                        .newWatchService()
  private val keys = new mutable.HashMap[WatchKey, Path]

  if (recursive) registerAll(path)

  /**
   * Register the given directory without subdirectories with the WatchService.
   */
  private def registerSingle(path: Path): Unit = {
    val key = path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE)

    keys(key) = path
  }

  /**
   * Register the given directory, and all its sub-directories, with the
   * WatchService.
   */
  private def registerAll(start: Path): Unit = {
    val _ = Files.walkFileTree(start, new SimpleFileVisitor[Path]() {
      override def preVisitDirectory(path: Path, attrs: BasicFileAttributes) = {
        if (!path.startsWith(start + "/tmp")) // do not track files in tmp/
          registerSingle(path)

        FileVisitResult.CONTINUE
      }
    })
  }

 /**
  * Start watching.
  */
  def start(): Unit = {
    while (running)
      try {
        val key = watchService.take()
        val dir = keys.getOrElse(key,
          throw new IOException("Not found a WatchKey. Something strange happened while watching local filesystem."))

        key.pollEvents()
           .foreach { case event: WatchEvent[Path] @unchecked if event.kind() != StandardWatchEventKinds.OVERFLOW =>
             val kind = event.kind()
             val path = event.context()
             val child = dir.resolve(path)

             if (recursive && kind == StandardWatchEventKinds.ENTRY_CREATE) {
               if (Files.isDirectory(child)) {
                 registerAll(child)
               }
               else if (Files.isRegularFile(child)) {
                 processEvent(kind, child)
               }
             }

             if (!key.reset()) {
               keys.remove(key) // invalid key
             }
           }

      } catch {
        case e: InterruptedException =>
          running = false
          watchService.close()
      }
  }
}