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
import java.io.IOException
import java.nio.file._
import java.nio.file.attribute._

// scala
import scala.collection.mutable
import scala.collection.JavaConversions._

/**
 * Utility class, watching for all files in given directory recursively.
 * Fire `onCreated` forwardFilePublished when some event occurs
 * It doesn't know whether file just created or write is finishers, just fire forwardFilePublished
 *
 * @see http://download.oracle.com/javase/tutorial/essential/io/examples/WatchDir.java
 * @param path root to be watched
 * @param onCreated forwardFilePublished to process fired event
 */
class FilesystemMonitor(path: Path, onCreated: Path => Unit) extends Runnable {

  @volatile private var running = true

  /**
   * Each `WatchKey` can be used only to watch a single directory (non-recursive)
   */
  private[this] val keys = new mutable.HashMap[WatchKey, Path]

  private val watchService = FileSystems.getDefault.newWatchService()

  registerPath(path)

  /**
   * Register the given directory without subdirectories with the WatchService.
   */
  private def registerSingle(path: Path): Unit = {
    val key = path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE)

    keys(key) = path
  }

  /**
   * Register the given directory, and all its sub-directories, with the
   * WatchService. Also if any of roots contain file - invoke `onCreated`
   * callback with it
   */
  private def registerPath(start: Path): Unit = {
    val _ = Files.walkFileTree(start, new SimpleFileVisitor[Path]() {
      override def visitFile(file: Path, attrs: BasicFileAttributes) = {
        if (Files.isRegularFile(file) && !file.getFileName.startsWith(".")) {
          onCreated(file)
        }
        FileVisitResult.CONTINUE
      }

      override def preVisitDirectory(path: Path, attrs: BasicFileAttributes) = {
        if (!path.startsWith(start + "/tmp")) // do not track files in tmp/
          registerSingle(path)

        FileVisitResult.CONTINUE
      }
    })
  }

  /**
   * Gracefully stop monitoring
   */
  def stop(): Unit = {
    running = false
    watchService.close()
  }

  /**
   * Handle event came from `WatchService`.
   * If new directory created - watch it, if new file - invoke [[onCreated]] forwardFilePublished
   * Skip all other events
   *
   * @param key WatchService key tied to this dir
   * @param dir watching root
   * @param event taken WatchService event
   */
  def processWatchEvent(key: WatchKey, dir: Path, event: WatchEvent[Path]): Unit = {
    event match {
      case e if e.kind == StandardWatchEventKinds.ENTRY_CREATE =>
        val path = e.context()
        val child = dir.resolve(path)

        if (Files.isDirectory(child)) {
          registerPath(child)                         // Watch newly created directory too
        } else if (Files.isRegularFile(child)) {
          onCreated(child)                            // Fire event
        }

        if (!key.reset()) {
          val _ = keys.remove(key)                    // Invalid key
        }
      case _ => ()                                    // Skip all other events
    }
  }

  /**
   * Main loop, awaiting for event from `WatchService`
   * Intercept each event and call `processWatchEvent` for it
   */
  def run(): Unit = {
    while (running)
      try {
        val key = watchService.take()
        val dir = keys.getOrElse(key, throw new IOException("Not found a WatchKey"))
        // Safe to cast WatchEvent http://stackoverflow.com/a/30177785/998523
        key.pollEvents().foreach(e => processWatchEvent(key, dir, e.asInstanceOf[WatchEvent[Path]]))
      } catch {
        case e: InterruptedException => stop()
        case e: ClosedWatchServiceException => stop()
      }
  }
}
