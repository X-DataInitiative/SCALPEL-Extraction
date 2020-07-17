// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.patients

import java.sql.Timestamp
import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.sources.Sources

class IrBenPatientsSuite extends SharedContext {

  "findBirthDate" should "return correct birth dates" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val irBen: Dataset[PatientIrBen] = Seq(
      PatientIrBen("Patient_01", 1, "1", "1975", Timestamp.valueOf("1975-01-01 00:00:00"), Some(null.asInstanceOf[Timestamp])),
      PatientIrBen("Patient_02", 2, "2", "1976", Timestamp.valueOf("1976-02-01 00:00:00"), Some(null.asInstanceOf[Timestamp]))
    ).toDS()

    val expected: Dataset[PatientIrBen] = Seq(
      PatientIrBen("Patient_01", 1, "1", "1975", Timestamp.valueOf("1975-01-01 00:00:00"), Some(null.asInstanceOf[Timestamp])),
      PatientIrBen("Patient_02", 2, "2", "1976", Timestamp.valueOf("1976-02-01 00:00:00"), Some(null.asInstanceOf[Timestamp]))
    ).toDS()

    //When
    val result = IrBenPatients.findPatientBirthDate(irBen)

    //Then
    assertDSs(result, expected)
  }

  "findGender" should "return correct gender" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: Dataset[PatientIrBen] = Seq(
      PatientIrBen("Patient_01", 1, "1", "1975", Timestamp.valueOf("1975-01-01 00:00:00"), Some(null.asInstanceOf[Timestamp])),
      PatientIrBen("Patient_02", 2, "2", "1976", Timestamp.valueOf("1976-02-01 00:00:00"), Some(null.asInstanceOf[Timestamp]))
    ).toDS()

    val expected: Dataset[PatientIrBen] = Seq(
      PatientIrBen("Patient_01", 1, "1", "1975", Timestamp.valueOf("1975-01-01 00:00:00"), Some(null.asInstanceOf[Timestamp])),
      PatientIrBen("Patient_02", 2, "2", "1976", Timestamp.valueOf("1976-02-01 00:00:00"), Some(null.asInstanceOf[Timestamp]))
    ).toDS()

    // When
    val result = IrBenPatients.findPatientGender(input)

    // Then
    assertDSs(result, expected)
  }

  "findDeathDate" should "find death dates correctly" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: Dataset[PatientIrBen] = Seq(
      PatientIrBen("Patient_01", 1, "1", "1975", Timestamp.valueOf("1975-01-01 00:00:00"), Some(null.asInstanceOf[Timestamp])),
      PatientIrBen("Patient_02", 1, "1", "1975", Timestamp.valueOf("1975-01-01 00:00:00"), Some(Timestamp.valueOf("2009-03-13 00:00:00"))),
      PatientIrBen("Patient_02", 1, "1", "1975", Timestamp.valueOf("1975-01-01 00:00:00"), Some(Timestamp.valueOf("2015-03-13 00:00:00"))),
      PatientIrBen("Patient_03", 1, "1", "1975", Timestamp.valueOf("1975-01-01 00:00:00"), Some(Timestamp.valueOf("1976-03-13 00:00:00"))),
      PatientIrBen("Patient_04", 1, "1", "1975", Timestamp.valueOf("1975-01-01 00:00:00"), Some(Timestamp.valueOf("2020-03-13 00:00:00")))
    ).toDS()

    val expected: Dataset[PatientIrBen] = Seq(
      PatientIrBen("Patient_01", 1, "1", "1975", Timestamp.valueOf("1975-01-01 00:00:00"), Some(null.asInstanceOf[Timestamp])),
      PatientIrBen("Patient_04", 1, "1", "1975", Timestamp.valueOf("1975-01-01 00:00:00"), Some(Timestamp.valueOf("2020-03-13 00:00:00"))),
      PatientIrBen("Patient_03", 1, "1", "1975", Timestamp.valueOf("1975-01-01 00:00:00"), Some(Timestamp.valueOf("1976-03-13 00:00:00"))),
      PatientIrBen("Patient_02", 1, "1", "1975", Timestamp.valueOf("1975-01-01 00:00:00"), Some(Timestamp.valueOf("2009-03-13 00:00:00"))),
      PatientIrBen("Patient_02", 1, "1", "1975", Timestamp.valueOf("1975-01-01 00:00:00"), Some(Timestamp.valueOf("2009-03-13 00:00:00")))
    ).toDS()

    // When
    val result = IrBenPatients.findPatientDeathDate(input)

    // Then
    assertDSs(result, expected)
  }

  "convert PatientIrBentoPatient" should "return Dataset of Patients" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: Dataset[PatientIrBen] = Seq(
      PatientIrBen("Patient_01", 1, "1", "1975", Timestamp.valueOf("1975-01-01 00:00:00"), Some(Timestamp.valueOf("2009-03-13 00:00:00"))),
      PatientIrBen("Patient_02", 2, "3", "1977", Timestamp.valueOf("1977-03-01 00:00:00"), Some(null.asInstanceOf[Timestamp]))
    ).toDS()

    val expected: Dataset[Patient] = Seq(
      Patient("Patient_01", 1, Timestamp.valueOf("1975-01-01 00:00:00"), Some(Timestamp.valueOf("2009-03-13 00:00:00"))),
      Patient("Patient_02", 2, Timestamp.valueOf("1977-03-01 00:00:00"), Some(null.asInstanceOf[Timestamp]))
    ).toDS()

    // When
    val result = IrBenPatients.fromDerivedPatienttoPatient(input)

    // Then
    assertDSs(result, expected)
  }

  "getInput" should "read file" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val irBen = spark.read.parquet("src/test/resources/test-input/IR_BEN_R.parquet")
    val sources = Sources(irBen = Some(irBen))

    val expected: Dataset[PatientIrBen] = Seq(
      PatientIrBen("Patient_01", 2, "01", "1975", Timestamp.valueOf("1975-01-01 00:00:00"), null),
      PatientIrBen("Patient_02", 1, "10", "1959", Timestamp.valueOf("1959-10-01 00:00:00"), Some(Timestamp.valueOf("2008-01-25 00:00:00")))
    ).toDS()

    // When
    val result = IrBenPatients.getInput(sources)

    // Then
    assertDSs(result, expected)
  }

  "extract" should "build patients with actual data" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val irBen = spark.read.parquet("src/test/resources/test-input/IR_BEN_R.parquet")
    val sources = Sources(irBen = Some(irBen))

    val expected: Dataset[Patient] = Seq(
      Patient("Patient_01", 2, Timestamp.valueOf("1975-01-01 00:00:00"), null),
      Patient("Patient_02", 1, Timestamp.valueOf("1959-10-01 00:00:00"), Some(Timestamp.valueOf("2008-01-25 00:00:00")))
    ).toDS()

    // When
    val result = IrBenPatients.extract(sources)

    // Then
    assertDSs(result, expected)
  }
}
