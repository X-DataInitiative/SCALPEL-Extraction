// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.patients

import java.sql.Timestamp
import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.sources.Sources

class DcirPatientsSuite extends SharedContext {

  "findPatientBirthDate" should "return a Dataset with the birth date for each patient" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: Dataset[PatientDcir] = Seq(
      PatientDcir("Patient_01", 2, 31, "1975", null, Timestamp.valueOf("2006-01-15 00:00:00"), None),
      PatientDcir("Patient_02", 1, 47, "1959", null, Timestamp.valueOf("2006-01-05 00:00:00"), Some(Timestamp.valueOf("2009-03-13 00:00:00")))
    ).toDS()

    val expected: Dataset[PatientDcir] = Seq(
      PatientDcir("Patient_01", 2, 31, "1975", Timestamp.valueOf("1975-01-01 00:00:00"), Timestamp.valueOf("2006-01-15 00:00:00"), None),
      PatientDcir("Patient_02", 1, 47, "1959", Timestamp.valueOf("1959-01-01 00:00:00"), Timestamp.valueOf("2006-01-05 00:00:00"), Some(Timestamp.valueOf("2009-03-13 00:00:00")))
    ).toDS()

    // When
    val result = DcirPatients.findPatientBirthDate(input)

    // Then
    assertDSs(result, expected)
  }

  "findGender" should "return a Dataset with the correct gender for each patient" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: Dataset[PatientDcir] = Seq(
      PatientDcir("Patient_01", 2, 31, "1975", null, Timestamp.valueOf("2006-01-15 00:00:00"), None),
      PatientDcir("Patient_01", 1, 31, "1975", null, Timestamp.valueOf("2006-01-15 00:00:00"), None),
      PatientDcir("Patient_01", 2, 31, "1975", null, Timestamp.valueOf("2006-01-30 00:00:00"), None),
      PatientDcir("Patient_02", 1, 47, "1975", null, Timestamp.valueOf("2006-01-05 00:00:00"), Some(Timestamp.valueOf("2009-03-13 00:00:00"))),
      PatientDcir("Patient_02", 1, 47, "1975", null, Timestamp.valueOf("2006-01-15 00:00:00"), Some(Timestamp.valueOf("2009-03-13 00:00:00"))),
      PatientDcir("Patient_02", 1, 47, "1975", null, Timestamp.valueOf("2006-01-30 00:00:00"), Some(Timestamp.valueOf("2009-03-13 00:00:00"))),
      PatientDcir("Patient_02", 1, 47, "1975", null, Timestamp.valueOf("2006-01-30 00:00:00"), Some(Timestamp.valueOf("2009-03-13 00:00:00")))
    ).toDS()

    val expected: Dataset[PatientDcir] = Seq(
      PatientDcir("Patient_01", 2, 31, "1975", null, Timestamp.valueOf("2006-01-15 00:00:00"), None),
      PatientDcir("Patient_01", 2, 31, "1975", null, Timestamp.valueOf("2006-01-15 00:00:00"), None),
      PatientDcir("Patient_01", 2, 31, "1975", null, Timestamp.valueOf("2006-01-30 00:00:00"), None),
      PatientDcir("Patient_02", 1, 47, "1975", null, Timestamp.valueOf("2006-01-05 00:00:00"), Some(Timestamp.valueOf("2009-03-13 00:00:00"))),
      PatientDcir("Patient_02", 1, 47, "1975", null, Timestamp.valueOf("2006-01-15 00:00:00"), Some(Timestamp.valueOf("2009-03-13 00:00:00"))),
      PatientDcir("Patient_02", 1, 47, "1975", null, Timestamp.valueOf("2006-01-30 00:00:00"), Some(Timestamp.valueOf("2009-03-13 00:00:00"))),
      PatientDcir("Patient_02", 1, 47, "1975", null, Timestamp.valueOf("2006-01-30 00:00:00"), Some(Timestamp.valueOf("2009-03-13 00:00:00")))
    ).toDS()

    // When
    val result = DcirPatients.findPatientGender(input)

    // Then
    assertDSs(result, expected)
  }

  "findDeathDate" should "return a Dataset with the correct death date for each patient" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: Dataset[PatientDcir] = Seq(
      PatientDcir("Patient_01", 1, 1, "1975", null, Timestamp.valueOf("1975-01-01 00:00:00"), Some(null.asInstanceOf[Timestamp])),
      PatientDcir("Patient_02", 1, 34, "1975", null, Timestamp.valueOf("1975-01-01 00:00:00"), Some(Timestamp.valueOf("2009-03-13 00:00:00"))),
      PatientDcir("Patient_02", 1, 30, "1975", null, Timestamp.valueOf("1975-01-01 00:00:00"), Some(Timestamp.valueOf("2015-03-13 00:00:00"))),
      PatientDcir("Patient_03", 1, 1, "1975", null, Timestamp.valueOf("1975-01-01 00:00:00"), Some(Timestamp.valueOf("1976-03-13 00:00:00"))),
      PatientDcir("Patient_04", 1, 45, "1975", null, Timestamp.valueOf("1975-01-01 00:00:00"), Some(Timestamp.valueOf("2020-03-13 00:00:00")))
    ).toDS()

    val expected: Dataset[PatientDcir] = Seq(
      PatientDcir("Patient_01", 1, 1, "1975", null, Timestamp.valueOf("1975-01-01 00:00:00"), Some(null.asInstanceOf[Timestamp])),
      PatientDcir("Patient_04", 1, 45, "1975", null, Timestamp.valueOf("1975-01-01 00:00:00"), Some(Timestamp.valueOf("2020-03-13 00:00:00"))),
      PatientDcir("Patient_03", 1, 1, "1975", null, Timestamp.valueOf("1975-01-01 00:00:00"), Some(Timestamp.valueOf("1976-03-13 00:00:00"))),
      PatientDcir("Patient_02", 1, 34, "1975", null, Timestamp.valueOf("1975-01-01 00:00:00"), Some(Timestamp.valueOf("2009-03-13 00:00:00"))),
      PatientDcir("Patient_02", 1, 30, "1975", null, Timestamp.valueOf("1975-01-01 00:00:00"), Some(Timestamp.valueOf("2009-03-13 00:00:00")))
    ).toDS()

    // When
    val result = DcirPatients.findPatientDeathDate(input)

    // Then
    assertDSs(result, expected)
  }

  "convert PatientDcirtoPatient" should "return Dataset of Patients" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: Dataset[PatientDcir] = Seq(
      PatientDcir("Patient_01", 2, 45, "1975", Timestamp.valueOf("1975-01-01 00:00:00"), Timestamp.valueOf("1975-01-01 00:00:00"), None),
      PatientDcir("Patient_02", 1, 49, "1959", Timestamp.valueOf("1959-01-01 00:00:00"), Timestamp.valueOf("1959-01-01 00:00:00"), Some(Timestamp.valueOf("2008-01-25 00:00:00")))
    ).toDS()

    val expected: Dataset[Patient] = Seq(
      Patient("Patient_01", 2, Timestamp.valueOf("1975-01-01 00:00:00"), None),
      Patient("Patient_02", 1, Timestamp.valueOf("1959-01-01 00:00:00"), Some(Timestamp.valueOf("2008-01-25 00:00:00")))
    ).toDS()

    // When
    val result = DcirPatients.fromDerivedPatienttoPatient(input)

    // Then
    assertDSs(result, expected)
  }

  "getInput" should "read file" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val dcir = spark.read.parquet("src/test/resources/test-input/DCIR.parquet")
    val sources = Sources(dcir = Some(dcir))

    val expected: Dataset[PatientDcir] = Seq(
      PatientDcir("Patient_01", 2, 31, "1975", null, null, null),
      PatientDcir("Patient_01", 2, 31, "1975", null, Timestamp.valueOf("2006-01-15 00:00:00"), null),
      PatientDcir("Patient_01", 2, 31, "1975", null, Timestamp.valueOf("2006-01-30 00:00:00"), null),
      PatientDcir("Patient_02", 1, 47, "1959", null, Timestamp.valueOf("2006-01-15 00:00:00"), Some(Timestamp.valueOf("2009-03-13 00:00:00"))),
      PatientDcir("Patient_02", 1, 47, "1959", null, Timestamp.valueOf("2006-01-30 00:00:00"), Some(Timestamp.valueOf("2009-03-13 00:00:00"))),
      PatientDcir("Patient_02", 1, 47, "1959", null, Timestamp.valueOf("2006-01-30 00:00:00"), Some(Timestamp.valueOf("2009-03-13 00:00:00"))),
      PatientDcir("Patient_02", 1, 47, "1959", null, Timestamp.valueOf("2006-01-05 00:00:00"), Some(Timestamp.valueOf("2009-03-13 00:00:00")))
    ).toDS()

    // When
    val result = DcirPatients.getInput(sources)

    // Then
    assertDSs(result, expected)
  }

  "extract" should "build patients with actual data" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val dcir = spark.read.parquet("src/test/resources/test-input/DCIR.parquet")
    val sources = Sources(dcir = Some(dcir))

    val expected: Dataset[Patient] = Seq(
      Patient("Patient_01", 2, Timestamp.valueOf("1975-01-01 00:00:00"), None),
      Patient("Patient_02", 1, Timestamp.valueOf("1959-01-01 00:00:00"), Some(Timestamp.valueOf("2009-03-13 00:00:00")))
    ).toDS()

    // When
    val result = DcirPatients.extract(sources)

    // Then
    assertDSs(result, expected)
  }
}
