package fr.polytechnique.cmap.cnam.etl.benchmark.local

import org.apache.log4j.{Level, Logger}
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.benchmark.BenchmarkBase

abstract class LocalSparkBenchmarkBase extends SharedContext with BenchmarkBase {

  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("fr.polytechnique").setLevel(Level.INFO)

  protected val numOfCols: Int = 100

}
