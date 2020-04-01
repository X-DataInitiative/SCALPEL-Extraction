// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.events.molecules

import java.sql.Timestamp
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, sum, udf, when}
import org.apache.spark.sql.types.{DoubleType, StringType, TimestampType}
import fr.polytechnique.cmap.cnam.etl.events.{Event, Molecule}
import fr.polytechnique.cmap.cnam.etl.extractors.Extractor
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.DrugEventsTransformerHelper

class DcirMoleculePurchases(config: MoleculePurchasesConfig) extends Extractor[Molecule, MoleculePurchasesConfig] {

  override def isInStudy(row: Row): Boolean = config.drugClasses.contains(row.getAs[String](Columns.Category))

  override def isInExtractorScope(row: Row): Boolean = !row.isNullAt(row.fieldIndex(Columns.EventDate)) &&
    row.getAs[Int](Columns.NBoxes) > 0

  override def builder(row: Row): Seq[Event[Molecule]] = Seq(
    Molecule(
      getPatientID(row),
      getValue(row),
      getWeight(row),
      getEventDate(row)
    )
  )

  def getPatientID(row: Row): String = row.getAs[String](Columns.PatientID)

  def getValue(row: Row): String = row.getAs[String](Columns.MoleculeName)

  def getWeight(row: Row): Double = row.getAs[Double](Columns.TotalDose)

  def getEventDate(row: Row): Timestamp = row.getAs[Timestamp](Columns.EventDate)

  override def getInput(sources: Sources): DataFrame = {
    val dcirInputColumns: List[Column] = List(
      col("NUM_ENQ").cast(StringType).as("patientID"),
      col("ER_PHA_F__PHA_PRS_IDE").cast(StringType).as("CIP07"),
      col("ER_PHA_F__PHA_PRS_C13").cast(StringType).as("CIP13"),
      // The DCIR parquet must contain this version of the column
      col("ER_PHA_F__PHA_ACT_QSN").as("nBoxes"),
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

    val groupCols: List[Column] = List(
      col("patientID"),
      col("moleculeName"),
      col("eventDate")
    )

    val irPha = sources.irPha.get
    val dosages = sources.dosages.get
    val moleculeMappingUDF = udf(DrugEventsTransformerHelper.moleculeMapping)
    val molecules = irPha.select(irPhaInputColumns: _*)
      .join(dosages.select(dosagesInputColumns: _*), Columns.CIP07)
      .withColumn(Columns.MoleculeName, moleculeMappingUDF(col(Columns.MoleculeName)))

    val df = sources.dcir.get
      .select(dcirInputColumns: _*)
      .withColumn(
        Columns.NBoxes, when(col(Columns.NBoxes) < 0, 0)
          .when(col(Columns.NBoxes) > config.maxBoxQuantity, 0)
          .otherwise(col(Columns.NBoxes))
      )
    // get CIP07 drug
    val joinedByCIP07 = df
      .where(col(Columns.CIP07).isNotNull)
      .drop(Columns.CIP13)
      .join(molecules, Columns.CIP07)

    //get CIP13 drug
    val joinedByCIP13 = df
      .where(col(Columns.CIP07).isNull)
      .drop(Columns.CIP07)
      .join(molecules, Columns.CIP13)
      .select(joinedByCIP07.columns.map(col): _*)

    val win = Window.partitionBy(groupCols: _*)
    joinedByCIP07.union(joinedByCIP13)
      .withColumn(Columns.TotalDose, sum(col(Columns.Dosage) * col(Columns.NBoxes)) over win) // Compute total dose
  }

  final object Columns extends Serializable {
    val PatientID = "patientID"
    val CIP07 = "CIP07"
    val CIP13 = "CIP13"
    val NBoxes = "nBoxes"
    val EventDate = "eventDate"
    val Category = "category"
    val MoleculeName = "moleculeName"
    val Dosage = "dosage"
    val TotalDose = "totalDose"
  }

  override def getCodes: MoleculePurchasesConfig = config
}

