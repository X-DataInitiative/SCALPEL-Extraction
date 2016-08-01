package fr.polytechnique.cnam.cmap.filtering

import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}

/**
  * @author Daniel de Paula
  */
trait Transformer[T] {
  def transform(sources: Sources): Dataset[T]
}

/**
  * Transformer object for patients data
  *
  * @author Daniel de Paula
  */
object PatientsTransformer extends Transformer[Patient] {

  def transform(sources: Sources): Dataset[Patient] = {
    val sqlContext = sources.dcir match { case Some(dcir: DataFrame) => dcir.sqlContext }

    // todo: implement transformation
    sqlContext.createDataset[Patient](Seq[Patient]())
  }
}

// todo: Add other transformer objects