package fr.polytechnique.cmap.cnam.filtering.extraction

import java.sql.Timestamp
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.mockito.Mockito.mock
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.filtering.Sources
import fr.polytechnique.cmap.cnam.filtering.events.Event
import fr.polytechnique.cmap.cnam.utilities.RichDataFrames
import fr.polytechnique.cmap.cnam.utilities.functions._

class DcirMoleculePurchasesSuite extends SharedContext {

  "addMoleculesInfo" should "return a new dataframe with molecule information" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: DataFrame = Seq(
      (Some("patient"), Some("3541848"), None),
      (Some("patient"), None, Some("3400935418487")),
      (Some("patient"), Some("3541848"), Some("3400935418487")),
      (Some("patient"), None, None)
    ).toDF("PatientID", "CIP07", "CIP13")
    val irPha: DataFrame = sqlContext.read.load("src/test/resources/expected/IR_PHA_R.parquet")
      .select(
        col("PHA_PRS_IDE").as("CIP07"),
        col("PHA_CIP_C13").as("CIP13")
      )
    val dosages: DataFrame = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/value_tables/DOSE_PER_MOLECULE.CSV")
      .select(
        col("PHA_PRS_IDE").as("CIP07"),
        col("MOLECULE_NAME"),
        col("TOTAL_MG_PER_UNIT").cast(IntegerType)
      )
    val expected = Seq(
      ("patient", "3541848", "3400935418487", "GLICLAZIDE", 900),
      ("patient", "3541848", "3400935418487", "GLICLAZIDE", 900),
      ("patient", "3541848", "3400935418487", "GLICLAZIDE", 900)
    ).toDF("PatientID", "CIP07", "CIP13", "MOLECULE_NAME", "TOTAL_MG_PER_UNIT")

    // When
    val moleculesDF = irPha.join(dosages, "CIP07")

    import DcirMoleculePurchases.DrugsDataFrame
    val result = input.addMoleculesInfo(moleculesDF).select(expected.columns.map(col): _*)

    // Then
    import RichDataFrames._
    result.show
    expected.show
    assert(result === expected)
  }

  "extract" should "return the correct data in a Dataset[Event[Molecule]] for known data" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val config: ExtractionConfig = mock(classOf[ExtractionConfig])
    val dcir: DataFrame = sqlContext.read.load("src/test/resources/expected/DCIR.parquet")
    val irPha: DataFrame = sqlContext.read.load("src/test/resources/expected/IR_PHA_R.parquet")
    val dosages: DataFrame = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/value_tables/DOSE_PER_MOLECULE.CSV")
      .select(
        col("PHA_PRS_IDE"),
        col("MOLECULE_NAME"),
        col("TOTAL_MG_PER_UNIT")
      )
    val sources = new Sources(
      dcir = Some(dcir),
      irPha = Some(irPha),
      dosages = Some(dosages)
    )

    // Note: there is a row in the dummy dataset where the field "EXE_SOI_DTD" is null.
    val expected = Seq(
      Event("Patient_01", "molecule", "SULFONYLUREA", 900.0, null.asInstanceOf[Timestamp], None),
      Event("Patient_01", "molecule", "SULFONYLUREA", 1800.0, makeTS(2006, 1, 15), None),
      Event("Patient_01", "molecule", "SULFONYLUREA", 900.0, makeTS(2006, 1, 30), None),
      Event("Patient_02", "molecule", "PIOGLITAZONE", 840.0, makeTS(2006, 1, 15), None),
      Event("Patient_02", "molecule", "PIOGLITAZONE", 4200.0, makeTS(2006, 1, 30), None),
      Event("Patient_02", "molecule", "PIOGLITAZONE", 1680.0, makeTS(2006, 1, 5), None)
    ).toDF

    // When
    val result = MoleculePurchases.extract(config, sources)

    // Then
    import RichDataFrames._
    result.printSchema
    expected.printSchema
    result.show
    expected.show
    assert(result.toDF === expected)
  }
}