package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

object DrugEventsTransformer extends Transformer[Event] {

  val drugCategories = List("A10") // Only anti-diabetics

  val dcirInputColumns: List[Column] = List(
    col("NUM_ENQ").as("patientID"),
    col("`ER_PHA_F.PHA_PRS_IDE`").as("CIP07"),
    col("`ER_PHA_F.PHA_PRS_C13`").as("CIP13"),
    col("`ER_PHA_F.PHA_ACT_QSN`").as("nBoxes"),
    col("EXE_SOI_DTD").as("eventDate")
  )

  val irPhaInputColumns: List[Column] = List(
    col("PHA_PRS_IDE").as("CIP07"),
    col("PHA_CIP_C13").as("CIP13"),
    col("PHA_ATC_C03").as("category")
  )

  val dosagesInputColumns: List[Column] = List(
    col("PHA_PRS_IDE").as("CIP07"),
    col("MOLECULE_NAME").as("moleculeName"),
    col("TOTAL_MG_PER_UNIT").as("dosage")
  )

  val outputColumns: List[Column] = List(
    col("patientID").as("patientID"),
    lit("molecule").as("category"),
    col("moleculeName").as("eventId"),
    col("totalDose").as("weight"),
    col("eventDate").cast(TimestampType).as("start"),
    lit(null).cast(TimestampType).as("end")
  )

  implicit class DrugsDataFrame(df: DataFrame) {

    def addMoleculesInfo(moleculesDF: DataFrame): DataFrame = {
      val joinedByCIP07 = df
        .where(col("CIP07").isNotNull)
        .drop("CIP13")
        .join(moleculesDF, "CIP07")

      val joinedByCIP13 = df
        .where(col("CIP07").isNull)
        .drop("CIP07")
        .join(moleculesDF, "CIP13")
        .select(joinedByCIP07.columns.map(col): _*)

      joinedByCIP07.unionAll(joinedByCIP13)
    }
  }

  def transform(sources: Sources): Dataset[Event] = {
    val dcir: DataFrame = sources.dcir.get.select(dcirInputColumns: _*)
    val irPha: DataFrame = sources.irPha.get.select(irPhaInputColumns: _*)
    val dosages: DataFrame = sources.dosages.get.select(dosagesInputColumns: _*)

    val moleculesInfo = irPha
      .where(col("category").isin(drugCategories: _*)) // Only anti-diabetics
      .join(dosages, "CIP07")

    import dcir.sqlContext.implicits._
    dcir
      .addMoleculesInfo(moleculesInfo) // Add molecule name and dosage
      .withColumn("totalDose", col("nBoxes") * col("dosage")) // Compute total dose
      .select(outputColumns: _*)
      .as[Event]
  }
}
