/*
 * Copyright (c) 2015 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.sauna.observers

import java.io.File
import java.nio.file.StandardWatchEventKinds.ENTRY_CREATE
import java.nio.file.FileSystems
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.WatchEvent

import scala.collection.JavaConversions._
import scala.io.Source.fromFile

/**
  * Observes files in local filesystem.
  */
class LocalObserver(observedDir: String) extends Observer {
  def watch(process: (Seq[String]) => Unit): Unit = {
    val watcher = FileSystems.getDefault.newWatchService()
    val observedDirWithSeparator = if (observedDir.endsWith(File.separator)) observedDir else observedDir + File.separator
    val dir = Paths.get(observedDirWithSeparator)
    dir.register(watcher, ENTRY_CREATE)

    new Thread {
      override def run(): Unit = {
        while (true) {
          val watchKey = watcher.take()
          watchKey.pollEvents().foreach { case event: WatchEvent[Path] @unchecked =>
            val file = new File(observedDirWithSeparator + event.context())
            val lines = fromFile(file).getLines().toSeq

            process(lines)

            if (!file.delete()) {
              System.err.println(s"Unable to delete ${file.getAbsolutePath}")
            }
          }

          if (!watchKey.reset())
            throw new Exception(s"'$observedDir' is not accessible anymore")
        }
      }
    }.start()
  }
}
