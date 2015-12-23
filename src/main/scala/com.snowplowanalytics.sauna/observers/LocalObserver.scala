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

import java.io.{IOException, FileInputStream, InputStream}
import java.nio.file._

import com.snowplowanalytics.sauna.loggers.{DirectoryWatcher, Logger}

/**
  * Observes files in local filesystem.
  */
class LocalObserver(observedDir: String) extends Observer { self: Logger =>
  def observe(process: (String, InputStream) => Unit): Unit = {

    def processEvent(event: WatchEvent.Kind[Path], path: Path): Unit = {
      if (event == StandardWatchEventKinds.ENTRY_CREATE) {
        self.notification(s"Detected new local file [$path].")
        val is = new FileInputStream(path.toFile)

        process("" + path, is)

        try {
          Files.delete(path)
        } catch { case e: IOException =>
          System.err.println(s"Unable to delete [$path].")
        }
      }
    }

    val watcher = new DirectoryWatcher(Paths.get(observedDir), processEvent)
    new Thread(watcher).start()
  }
}
