// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.patients

import java.sql.Timestamp
import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.sources.Sources

class McoPatientsSuite extends SharedContext {

  "findBirthDate" should "return the same dataset" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: Dataset[PatientMco] = Seq(
      PatientMco("Patient_01", 9, "01", "2009", 1, null, Some(Timestamp.valueOf("2009-01-01 00:00:00"))),
      PatientMco("Patient_02", 9, "03", "2010", 2, null, Some(Timestamp.valueOf("2010-03-01 00:00:00")))
    ).toDS()

    val expected: Dataset[PatientMco] = Seq(
      PatientMco("Patient_01", 9, "01", "2009", 1, null, Some(Timestamp.valueOf("2009-01-01 00:00:00"))),
      PatientMco("Patient_02", 9, "03", "2010", 2, null, Some(Timestamp.valueOf("2010-03-01 00:00:00")))
    ).toDS()

    // When
    val result: Dataset[PatientMco] = McoPatients.findPatientBirthDate(input)

    // Then
    assertDSs(result, expected)
  }

  "findGender" should "return the same dataset" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: Dataset[PatientMco] = Seq(
      PatientMco("Patient_01", 9, "01", "2009", 1, null, Some(Timestamp.valueOf("2009-01-01 00:00:00"))),
      PatientMco("Patient_02", 9, "03", "2010", 2, null, Some(Timestamp.valueOf("2010-03-01 00:00:00")))
    ).toDS()

    val expected: Dataset[PatientMco] = Seq(
      PatientMco("Patient_01", 9, "01", "2009", 1, null, Some(Timestamp.valueOf("2009-01-01 00:00:00"))),
      PatientMco("Patient_02", 9, "03", "2010", 2, null, Some(Timestamp.valueOf("2010-03-01 00:00:00")))
    ).toDS()

    // When
    val result: Dataset[PatientMco] = McoPatients.findPatientGender(input)

    // Then
    assertDSs(result, expected)
  }

  "findDeathDate" should "choose minimum death date if a patient has more than one death dates" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: Dataset[PatientMco] = Seq(
      PatientMco("Patient_01", 1, "2", "1985", 1, null, None),
      PatientMco("Patient_02", 9, "3", "1986", 1, null, Some(Timestamp.valueOf("1986-03-01 00:00:00"))),
      PatientMco("Patient_03", 9, "4", "1980", 1, null, Some(Timestamp.valueOf("1980-04-01 00:00:00"))),
      PatientMco("Patient_03", 9, "4", "1984", 1, null, Some(Timestamp.valueOf("1984-04-01 00:00:00"))),
      PatientMco("Patient_04", 3, "5", "1995", 1, null, None)
    ).toDS()

    val expected: Dataset[PatientMco] = Seq(
      PatientMco("Patient_02", 9, "3", "1986", 1, null, Some(Timestamp.valueOf("1986-03-01 00:00:00"))),
      PatientMco("Patient_03", 9, "4", "1980", 1, null, Some(Timestamp.valueOf("1980-04-01 00:00:00")))
    ).toDS()

    // When
    val result: Dataset[PatientMco] = McoPatients.findPatientDeathDate(input)

    // Then
    assertDSs(result, expected)
  }

  "convert PatientMcotoPatient" should "return Dataset of Patients" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: Dataset[PatientMco] = Seq(
      PatientMco("Patient_01", 9, "1", "2009", 1, null, Some(Timestamp.valueOf("2009-01-01 00:00:00"))),
      PatientMco("Patient_02", 9, "3", "2000", 2, null, Some(Timestamp.valueOf("2000-03-01 00:00:00")))
    ).toDS()

    val expected: Dataset[Patient] = Seq(
      Patient("Patient_01", 1, null, Some(Timestamp.valueOf("2009-01-01 00:00:00"))),
      Patient("Patient_02", 2, null, Some(Timestamp.valueOf("2000-03-01 00:00:00")))
    ).toDS()

    // When
    val result = McoPatients.fromDerivedPatienttoPatient(input)

    // Then
    assertDSs(result, expected)
  }

  "getInput" should "read file" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val mco = spark.read.parquet("src/test/resources/test-input/MCO.parquet")
    val sources = Sources(mco = Some(mco))

    val expected: Dataset[PatientMco] = Seq(
      PatientMco("Patient_02", 5, "2", "2007", 0, null, Some(Timestamp.valueOf("2007-02-01 00:00:00"))),
      PatientMco("Patient_02", 5, "2", "2007", 0, null, Some(Timestamp.valueOf("2007-02-01 00:00:00"))),
      PatientMco("Patient_02", 5, "1", "2006", 0, null, Some(Timestamp.valueOf("2006-01-01 00:00:00"))),
      PatientMco("Patient_02", 5, "1", "2006", 0, null, Some(Timestamp.valueOf("2006-01-01 00:00:00"))),
      PatientMco("Patient_02", 5, "3", "2008", 0, null, Some(Timestamp.valueOf("2008-03-01 00:00:00"))),
      PatientMco("Patient_02", 5, "3", "2008", 0, null, Some(Timestamp.valueOf("2008-03-01 00:00:00")))
    ).toDS()

    // When
    val result = McoPatients.getInput(sources)

    // Then
    assertDSs(result, expected)
  }

  "extract" should "build patients with actual data" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val mco = spark.read.parquet("src/test/resources/test-input/MCO.parquet")
    val sources = Sources(mco = Some(mco))

    val expected: Dataset[Patient] = Seq.empty[Patient].toDS()

    // When
    val result = McoPatients.extract(sources)

    // Then
    assertDSs(result, expected)
  }
}