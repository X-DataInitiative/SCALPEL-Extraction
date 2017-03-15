package fr.polytechnique.cmap.cnam.etl.old_root

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.DrugEventsTransformerHelper

object DrugEventsTransformer extends Transformer[Event] {

  val drugCategories: List[String] = FilteringConfig.drugCategories

  val dcirInputColumns: List[Column] = List(
    col("NUM_ENQ").cast(StringType).as("patientID"),
    col("`ER_PHA_F.PHA_PRS_IDE`").cast(StringType).as("CIP07"),
    col("`ER_PHA_F.PHA_PRS_C13`").cast(StringType).as("CIP13"),
    col("`ER_PHA_F.PHA_ACT_QSN`").as("nBoxes"),
    col("EXE_SOI_DTD").as("eventDate")
  )

  val irPhaInputColumns: List[Column] = List(
    col("PHA_PRS_IDE").cast(StringType).as("CIP07"),
    col("PHA_CIP_C13").cast(StringType).as("CIP13"),
    col("PHA_ATC_C03").as("category")
  )

  val dosagesInputColumns: List[Column] = List(
    col("PHA_PRS_IDE").cast(StringType).as("CIP07"),
    col("MOLECULE_NAME").as("moleculeName"),
    col("TOTAL_MG_PER_UNIT").as("dosage")
  )

  val groupCols: List[Column] = List(col("patientID"), col("moleculeName"), col("eventDate"))

  val outputColumns: List[Column] = List(
    col("patientID").as("patientID"),
    lit("molecule").as("category"),
    col("moleculeName").as("eventId"),
    col("totalDose").as("weight"),
    col("eventDate").cast(TimestampType).as("start"),
    lit(null).cast(TimestampType).as("end")
  )

  implicit class DrugsDataFrame(df: DataFrame) {

    def addMoleculesInfo(molecules: DataFrame): DataFrame = {
      val moleculesDF = broadcast(molecules)
      val joinedByCIP07 = df
        .where(col("CIP07").isNotNull)
        .drop("CIP13")
        .join(moleculesDF, "CIP07")

      val joinedByCIP13 = df
        .where(col("CIP07").isNull)
        .drop("CIP07")
        .join(moleculesDF, "CIP13")
        .select(joinedByCIP07.columns.map(col): _*)

      joinedByCIP07.union(joinedByCIP13)
    }
  }

  def transform(sources: Sources): Dataset[Event] = {
    val irPha: DataFrame = sources.irPha.get.select(irPhaInputColumns: _*)
    val dosages: DataFrame = sources.dosages.get.select(dosagesInputColumns: _*)
    val sqlContext = irPha.sqlContext

    val moleculeMappingUDF = udf(DrugEventsTransformerHelper.moleculeMapping)

    val moleculesInfo = irPha
      .where(col("category").isin(drugCategories: _*)) // Only anti-diabetics
      .join(broadcast(dosages), "CIP07")
      .withColumn("moleculeName", moleculeMappingUDF(col("moleculeName")))
      .persist()

    val CIP07List = sqlContext.sparkContext.broadcast(
      moleculesInfo.select("CIP07").distinct.where(col("CIP07").isNotNull).collect.map(_.getString(0))
    )
    val CIP13List = sqlContext.sparkContext.broadcast(
      moleculesInfo.select("CIP13").distinct.where(col("CIP13").isNotNull).collect.map(_.getString(0))
    )

    val dcir: DataFrame = sources.dcir.get.select(dcirInputColumns: _*)
      .na.drop("any", Seq("CIP07", "CIP13"))
      .where(col("CIP07").isin(CIP07List.value: _*) || col("CIP13").isin(CIP13List.value: _*))
      .persist()

    import dcir.sqlContext.implicits._
    val result = dcir
      .addMoleculesInfo(moleculesInfo) // Add molecule name and dosage
      .withColumn("totalDose", col("nBoxes") * col("dosage")) // Compute total dose
      .groupBy(groupCols: _*).agg(sum("totalDose").as("totalDose"))
      .select(outputColumns: _*)
      .as[Event]

    moleculesInfo.unpersist()
    dcir.unpersist()
    result
  }
}
