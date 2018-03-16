package fr.polytechnique.cmap.cnam.etl

import fr.polytechnique.cmap.cnam.etl.sources._
import fr.polytechnique.cmap.cnam.study.StudyConfig.InputPaths
import org.apache.spark.sql.SQLContext

package object implicits {

  /**
    * Implicit class for reading data using an sqlContext directly (not as an argument)
    */
  implicit class SourceReader(sqlContext: SQLContext) {

    def readSources(paths: InputPaths): Sources = {
      Sources.read(sqlContext, paths)
    }
  }
}
