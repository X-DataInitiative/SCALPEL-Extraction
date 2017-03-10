package fr.polytechnique.cmap.cnam.etl

import org.apache.spark.sql.{Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.etl.old_root.FilteringConfig.InputPaths
import fr.polytechnique.cmap.cnam.etl.old_root._
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.sources._

package object implicits {

  /**
    * Implicit class for writing the events to a parquet file
    *
    * TODO: We should decide if this class should stay here or in an object "Writer", importable by
    * doing "import Writer._"
    */
  implicit class FlatEventWriter(data: Dataset[Event]) {

    def writeFlatEvent(patient: Dataset[Patient], path: String): Unit = {
      import data.sqlContext.implicits._

      data.as("left")
        .joinWith(patient.as("right"), $"left.patientID" === $"right.patientID")
        .map((FlatEvent.merge _).tupled)
        .toDF()
        .write
        .parquet(path)
    }
  }

  /**
    * Implicit class for reading data using an sqlContext directly (not as an argument)
    */
  implicit class SourceReader(sqlContext: SQLContext) {

    def readSources(paths: InputPaths): Sources = {
      Sources.read(sqlContext, paths)
    }
  }
}
