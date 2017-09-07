package fr.polytechnique.cmap.cnam.etl.extractors.drugs

import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types.{StringType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.etl.events.{Drug, Event}

object NaiveDefinition {
  def apply(dcir: DataFrame, drugConfig: DrugConfig): NaiveDefinition = {
    new NaiveDefinition(dcir, drugConfig)
  }
}

class NaiveDefinition(dcir: DataFrame, drugConfig: DrugConfig) {

  def extract: Dataset[Event[Drug]] = {

    val drugName: String = drugConfig.name
    val drugCodes: List[String] = drugConfig.cip13Codes
    val drugDosage: Double = 0.0

    val neededColumns: List[Column] = List(
      col("NUM_ENQ").cast(StringType).as("patientID"),
      col("ER_PHA_F__PHA_PRS_C13").cast(StringType).as("CIP13"),
      col("EXE_SOI_DTD").cast(TimestampType).as("eventDate")
    )

    val sqlCtx = dcir.sqlContext
    import sqlCtx.implicits._
    dcir
      .select(neededColumns: _*)
//      .where(col("eventDate").isNotNull)
      .na.drop(Seq("eventDate", "CIP13"))
      .where(col("CIP13").isin(drugCodes: _*))
      .withColumn("drugName", lit(drugName))
      .withColumn("dosage", lit(drugDosage))
      .map(Drug.fromRow(_, nameCol = "drugName"))
  }
}
