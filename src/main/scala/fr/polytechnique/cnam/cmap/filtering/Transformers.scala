package fr.polytechnique.cnam.cmap.filtering

import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * @author Daniel de Paula
  */
trait Transformer[T] {
  def transform(sources: Sources): Dataset[T]
}

/**
  * Transformer object for patients data
  * Note that all transformers should cache the DataFrames that are going to be used
  *
  * @author Daniel de Paula
  */
object PatientsTransformer extends Transformer[Patient] {

  def transform(sources: Sources): Dataset[Patient] = {
    val dcir: DataFrame = sources.dcir.get.cache()
    val sqlContext = dcir.sqlContext
    import sqlContext.implicits._

    // todo: implement transformation
    sqlContext.createDataset[Patient](Seq[Patient]())
  }
}

// todo: Implement other transformer objects.
// Note that all transformers should cache the DataFrames that are going to be used */

/*
object DrugEventsTransformer extends Transformer[FlatEvent] {

  def transform(sources: Sources): Dataset[FlatEvent] = {

  }
}
*/

/*
object DiseaseEventsTransformer extends Transformer[FlatEvent] {

  def transform(sources: Sources): Dataset[FlatEvent] = {

  }
}
 */
