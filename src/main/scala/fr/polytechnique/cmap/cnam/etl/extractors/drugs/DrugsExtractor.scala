package fr.polytechnique.cmap.cnam.etl.extractors.drugs

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, TimestampType}
import fr.polytechnique.cmap.cnam.etl.events.{Drug, Event}
import fr.polytechnique.cmap.cnam.etl.sources.Sources

class DrugsExtractor(drugConfig: DrugConfig) extends java.io.Serializable{

  def formatSource(sources : Sources): Dataset[DrugPurchaseDAO] = {

    val neededColumns: List[Column] = List(
      col("NUM_ENQ").cast(StringType).as("patientID"),
      col("ER_PHA_F__PHA_PRS_C13").cast(StringType).as("CIP13"),
      col("PHA_ATC_C07").cast(StringType).as("ATC5"),
      col("EXE_SOI_DTD").cast(TimestampType).as("eventDate"),
      col("molecule_combination").cast(StringType).as("molecules"),
      col("PHA_CND_TOP").cast(StringType).as("conditioning")
    )

    lazy val irPhaR = sources.irPha.get
    lazy val dcir = sources.dcir.get
    val spark: SparkSession = dcir.sparkSession
    import spark.implicits._

    lazy val df: DataFrame = dcir.join(irPhaR, dcir.col("ER_PHA_F__PHA_PRS_C13") === irPhaR.col("PHA_CIP_C13"))

    df
      .select(neededColumns: _*)
      .withColumn("conditioning", when(col("conditioning") === "GC", 1).otherwise(2))
      .na.drop(Seq("eventDate", "CIP13", "ATC5"))
      .as[DrugPurchaseDAO]
  }

  def extract(sources : Sources): Dataset[Event[Drug]] = {
    val drugPurchases = formatSource(sources)
    val spark: SparkSession = drugPurchases.sparkSession
    import spark.implicits._
    drugPurchases.flatMap(purchase => drugConfig.level(purchase, drugConfig.families))
  }
}
