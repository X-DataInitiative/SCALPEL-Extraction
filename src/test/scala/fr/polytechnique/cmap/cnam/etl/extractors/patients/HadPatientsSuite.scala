package fr.polytechnique.cmap.cnam.etl.extractors.patients

import java.sql.Timestamp
import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.sources.Sources

class HadPatientsSuite extends SharedContext {

  "findBirthDate" should "return the same dataset" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: Dataset[PatientHad] = Seq(
      PatientHad("Patient_01", 9, "01", "2009", 1, null, Some(Timestamp.valueOf("2009-01-01 00:00:00"))),
      PatientHad("Patient_02", 9, "03", "2010", 2, null, Some(Timestamp.valueOf("2010-03-01 00:00:00")))
    ).toDS()

    val expected: Dataset[PatientHad] = Seq(
      PatientHad("Patient_01", 9, "01", "2009", 1, null, Some(Timestamp.valueOf("2009-01-01 00:00:00"))),
      PatientHad("Patient_02", 9, "03", "2010", 2, null, Some(Timestamp.valueOf("2010-03-01 00:00:00")))
    ).toDS()

    // When
    val result: Dataset[PatientHad] = HadPatients.findPatientBirthDate(input)

    // Then
    assertDSs(result, expected)
  }

  "findGender" should "return the same dataset" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: Dataset[PatientHad] = Seq(
      PatientHad("Patient_01", 9, "01", "2009", 1, null, Some(Timestamp.valueOf("2009-01-01 00:00:00"))),
      PatientHad("Patient_02", 9, "03", "2010", 2, null, Some(Timestamp.valueOf("2010-03-01 00:00:00")))
    ).toDS()

    val expected: Dataset[PatientHad] = Seq(
      PatientHad("Patient_01", 9, "01", "2009", 1, null, Some(Timestamp.valueOf("2009-01-01 00:00:00"))),
      PatientHad("Patient_02", 9, "03", "2010", 2, null, Some(Timestamp.valueOf("2010-03-01 00:00:00")))
    ).toDS()

    // When
    val result: Dataset[PatientHad] = HadPatients.findPatientGender(input)

    // Then
    assertDSs(result, expected)
  }

  "findDeathDate" should "choose minimum death date if a patient has more than one death dates" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: Dataset[PatientHad] = Seq(
      PatientHad("Patient_01", 1, "2", "1985", 1, null, None),
      PatientHad("Patient_02", 9, "3", "1986", 1, null, Some(Timestamp.valueOf("1986-03-01 00:00:00"))),
      PatientHad("Patient_03", 9, "4", "1980", 1, null, Some(Timestamp.valueOf("1980-04-01 00:00:00"))),
      PatientHad("Patient_03", 9, "4", "1984", 1, null, Some(Timestamp.valueOf("1984-04-01 00:00:00"))),
      PatientHad("Patient_04", 3, "5", "1995", 1, null, None)
    ).toDS()

    val expected: Dataset[PatientHad] = Seq(
      PatientHad("Patient_02", 9, "3", "1986", 1, null, Some(Timestamp.valueOf("1986-03-01 00:00:00"))),
      PatientHad("Patient_03", 9, "4", "1980", 1, null, Some(Timestamp.valueOf("1980-04-01 00:00:00")))
    ).toDS()

    // When
    val result: Dataset[PatientHad] = HadPatients.findPatientDeathDate(input)

    // Then
    assertDSs(result, expected)
  }

  "convert PatientHadtoPatient" should "return Dataset of Patients" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: Dataset[PatientHad] = Seq(
      PatientHad("Patient_01", 9, "01", "2009", 1, null, Some(Timestamp.valueOf("2009-01-01 00:00:00"))),
      PatientHad("Patient_02", 9, "03", "2000", 2, null, Some(Timestamp.valueOf("2000-03-01 00:00:00")))
    ).toDS()

    val expected: Dataset[Patient] = Seq(
      Patient("Patient_01", 1, null, Some(Timestamp.valueOf("2009-01-01 00:00:00"))),
      Patient("Patient_02", 2, null, Some(Timestamp.valueOf("2000-03-01 00:00:00")))
    ).toDS()

    // When
    val result = HadPatients.fromDerivedPatienttoPatient(input)

    // Then
    assertDSs(result, expected)
  }

  "getInput" should "read file" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val had = spark.read.parquet("src/test/resources/test-input/HAD.parquet")
    val sources = Sources(had = Some(had))

    val expected: Dataset[PatientHad] = Seq(
      PatientHad("patient01", 8, "1", "2019", 0, null, Some(Timestamp.valueOf("2019-01-01 00:00:00"))),
      PatientHad("patient01", -1, "1", "2019", 0, null, Some(Timestamp.valueOf("2019-01-01 00:00:00"))),
      PatientHad("patient02", 0, "1", "2019", 0, null, Some(Timestamp.valueOf("2019-01-01 00:00:00"))),
      PatientHad("patient02", 0, "1", "2019", 0, null, Some(Timestamp.valueOf("2019-01-01 00:00:00")))
    ).toDS()

    // When
    val result = HadPatients.getInput(sources)

    // Then
    assertDSs(result, expected)
  }

  "extract" should "build patients with actual data" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val had = spark.read.parquet("src/test/resources/test-input/HAD.parquet")
    val sources = Sources(had = Some(had))

    val expected: Dataset[Patient] = Seq.empty[Patient].toDS()

    // When
    val result = HadPatients.extract(sources)

    // Then
    assertDSs(result, expected)
  }

}