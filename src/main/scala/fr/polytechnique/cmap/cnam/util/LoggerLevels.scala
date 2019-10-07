// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.util

import org.apache.log4j.{Level, Logger}

trait LoggerLevels {
  Logger.getRootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("fr.polytechnique").setLevel(Level.INFO)
}
