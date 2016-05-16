/*
 * Copyright (c) 2012-2016 Snowplow Analytics Ltd. All rights reserved.
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
import java.nio.file._

// scala
import scala.collection.mutable
import scala.concurrent.duration._

// akka
import akka.actor._

// sauna
import Observer.{ LocalFilePublished, DeleteLocalFile }

/**
 * Local observer responsible for working with local system:
 * + signalling when new file is created on local filesystem
 * + maintaining list of known files
 * + deleting files
 *
 * @param root directory to track
 */
class LocalObserver(root: Path) extends Actor with Observer {
  import LocalObserver._

  import context.dispatcher

  /**
   * Sizes of each created file to track when copying is finishers
   */
  private val sizes = new mutable.HashMap[Path, Size]()

  /**
   * Run `FilesystemMonitor` in separate thread
   * It will message this actor about created files (but they can be on-write
   * at that moment)
   */
  // It's better to throw exceptions in monitor rather than stop gracefully
  // then LocalObserver will be restarted rather than hang
  private val monitor = new FilesystemMonitor(root, onCreated)
  private val watcherThread = new Thread(monitor)
  watcherThread.setDaemon(true)
  watcherThread.start()

  /**
   * Schedule check of sizes every 2 seconds
   * If after this period, file size hasn't been changed - its ready to be processed
   */
  private val tick = context.system.scheduler.schedule(1000.millis, 2000.millis, self, Tick)

  /**
   * Callback supposed to be called in `FilesystemMonitor` thread
   * when new file appears
   * Create message and send it to itself
   *
   * @param path local filesystem path to the file
   */
  def onCreated(path: Path): Unit = {
    self ! FileCreated(path)
  }

  /**
   * Update `sizes` hashmap with current size of file and return result
   * If size remains same - file write is completed, otherwise put new size
   *
   * @param file local filesystem path to a file
   * @param oldSize size currently written in `sizes`
   */
  def updateFile(file: Path, oldSize: Long): UpdateResult = {
    val newSize = Files.size(file)

    if (oldSize == newSize) {
      sizes.remove(file)
      Ready
    } else if (oldSize > newSize) {
      Error(s"Old size [$oldSize] is bigger than new [$newSize]", file)
    } else {
      sizes.put(file, newSize)
      Updated
    }
  }

  def receive = {
    case Tick =>
      sizes.foreach { case (path, size) =>
        updateFile(path, size) match {
          case Ready =>
            notify(s"Detected new local file [${path.toString}]")
            context.parent ! LocalFilePublished(path, self)
          case Updated => ()
          case Error(_, _) => ()
        }
      }

    case FileCreated(path) =>
      val newSize = Files.size(path)
      sizes.put(path, newSize)

    case deleteFile: DeleteLocalFile =>
      deleteFile.run()
  }

  /**
   * Clean-up resources
   */
  override def postStop(): Unit = {
    tick.cancel()
    watcherThread.interrupt()
  }
}

object LocalObserver {

  type Size = Long

  object Tick

  /**
   * File just appeared on filesystem.
   * This is private because its signalling from monitor thread only about
   * appearance of file, it can be writing at this moment `ObserverBatchEvent`
   * event must be used to signal responder
   */
  private[observers] case class FileCreated(path: Path)

  sealed trait UpdateResult extends Product with Serializable
  case object Updated extends UpdateResult
  case object Ready extends UpdateResult
  case class Error(message: String, path: Path) extends UpdateResult

  def props(path: Path): Props =
    Props(new LocalObserver(path))

  def props(path: String): Props =
    props(Paths.get(path))
}

