package fr.polytechnique.cnam.cmap.filtering

import org.apache.spark.sql.{Dataset, SQLContext}

/**
  * @author Daniel de Paula
  */
class PatientReader(sqlContext: SQLContext) {

  def read(path: String): Dataset[Patient] = {
    // todo: read patients
    val patients = Seq[Patient]()
    sqlContext.createDataset(patients)
  }
}
