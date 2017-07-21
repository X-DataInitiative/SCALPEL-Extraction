package fr.polytechnique.cmap.cnam.etl.extractors.molecules

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.etl.events.{Event, Molecule}
import fr.polytechnique.cmap.cnam.util.DrugEventsTransformerHelper

private[molecules] object DcirMoleculePurchases {

  implicit class DrugsDataFrame(df: DataFrame) {

    def filterBoxQuantities(maxQuantity: Int): DataFrame = {
      df.where(col("nBoxes") > 0 and col("nBoxes") <= maxQuantity)
    }

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

  def extract(
      dcir: DataFrame,
      irPha: DataFrame,
      dosages: DataFrame,
      drugClasses: List[String],
      maxBoxQuantity: Int): Dataset[Event[Molecule]] = {

    val sqlContext = dcir.sqlContext

    val dcirInputColumns: List[Column] = List(
      col("NUM_ENQ").cast(StringType).as("patientID"),
      col("ER_PHA_F__PHA_PRS_IDE").cast(StringType).as("CIP07"),
      col("ER_PHA_F__PHA_PRS_C13").cast(StringType).as("CIP13"),
      col("ER_PHA_F__PHA_ACT_QSN").as("nBoxes"), // The DCIR parquet must contain this version of the column
      col("EXE_SOI_DTD").cast(TimestampType).as("eventDate")
    )

    val irPhaInputColumns: List[Column] = List(
      col("PHA_PRS_IDE").cast(StringType).as("CIP07"),
      col("PHA_CIP_C13").cast(StringType).as("CIP13"),
      col("PHA_ATC_C03").as("category")
    )

    val dosagesInputColumns: List[Column] = List(
      col("PHA_PRS_IDE").cast(StringType).as("CIP07"),
      col("MOLECULE_NAME").as("moleculeName"),
      col("TOTAL_MG_PER_UNIT").cast(DoubleType).as("dosage")
    )

    val groupCols: List[Column] = List(col("patientID"), col("moleculeName"), col("eventDate"))

    val moleculeMappingUDF = udf(DrugEventsTransformerHelper.moleculeMapping)

    val moleculesInfo = irPha.select(irPhaInputColumns: _*)
      .where(col("category").isin(drugClasses: _*)) // Only anti-diabetics
      .join(broadcast(dosages.select(dosagesInputColumns: _*)), "CIP07")
      .withColumn("moleculeName", moleculeMappingUDF(col("moleculeName")))
      .persist()

    val CIP07List = sqlContext.sparkContext.broadcast(
      moleculesInfo.select("CIP07").distinct.where(col("CIP07").isNotNull).collect.map(_.getString(0))
    )
    val CIP13List = sqlContext.sparkContext.broadcast(
      moleculesInfo.select("CIP13").distinct.where(col("CIP13").isNotNull).collect.map(_.getString(0))
    )

    val validatedDcir: DataFrame = dcir
      .select(dcirInputColumns: _*)
      .where(col("eventDate").isNotNull)
      .filterBoxQuantities(maxBoxQuantity)
      .na.drop("any", Seq("CIP07", "CIP13"))
      .where(col("CIP07").isin(CIP07List.value: _*) || col("CIP13").isin(CIP13List.value: _*))
      .persist()

    import sqlContext.implicits._
    val result = validatedDcir
      .addMoleculesInfo(moleculesInfo) // Add molecule name and dosage
      .withColumn("totalDose", col("dosage") * col("nBoxes")) // Compute total dose
      .groupBy(groupCols: _*)
      .agg(sum("totalDose").as("totalDose"))
      .map(Molecule.fromRow(_, nameCol = "moleculeName", dosageCol = "totalDose"))

    moleculesInfo.unpersist()
    validatedDcir.unpersist()
    result
  }
}