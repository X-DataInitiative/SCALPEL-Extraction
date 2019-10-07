// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl

import org.apache.spark.sql.SQLContext
import fr.polytechnique.cmap.cnam.etl.config.study.StudyConfig.InputPaths
import fr.polytechnique.cmap.cnam.etl.sources._

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
