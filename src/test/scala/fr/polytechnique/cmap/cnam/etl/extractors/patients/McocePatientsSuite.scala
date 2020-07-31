// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.patients

import java.sql.Timestamp
import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.sources.Sources

class McocePatientsSuite extends SharedContext {

  "findPatientBirthDate" should "return a Dataset with the birth date for each patient" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: Dataset[PatientMcoce] = Seq(
      PatientMcoce("200410", 1, Some(79), null, Timestamp.valueOf("2014-04-18 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(73), null, Timestamp.valueOf("2014-01-09 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(73), null, Timestamp.valueOf("2014-02-11 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(74), null, Timestamp.valueOf("2014-07-18 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(74), null, Timestamp.valueOf("2014-12-12 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(74), null, Timestamp.valueOf("2014-04-15 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(74), null, Timestamp.valueOf("2014-10-27 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(74), null, Timestamp.valueOf("2014-04-04 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(74), null, Timestamp.valueOf("2014-11-06 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(74), null, Timestamp.valueOf("2014-05-02 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(74), null, Timestamp.valueOf("2014-09-26 00:00:00"), None)
    ).toDS()

    val expected: Dataset[PatientMcoce] = Seq(
      PatientMcoce("200410", 1, Some(79), Timestamp.valueOf("1935-04-01 00:00:00"), Timestamp.valueOf("2014-04-18 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(73), Timestamp.valueOf("1940-03-01 00:00:00"), Timestamp.valueOf("2014-01-09 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(73), Timestamp.valueOf("1940-03-01 00:00:00"), Timestamp.valueOf("2014-02-11 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(74), Timestamp.valueOf("1940-03-01 00:00:00"), Timestamp.valueOf("2014-07-18 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(74), Timestamp.valueOf("1940-03-01 00:00:00"), Timestamp.valueOf("2014-12-12 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(74), Timestamp.valueOf("1940-03-01 00:00:00"), Timestamp.valueOf("2014-04-15 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(74), Timestamp.valueOf("1940-03-01 00:00:00"), Timestamp.valueOf("2014-10-27 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(74), Timestamp.valueOf("1940-03-01 00:00:00"), Timestamp.valueOf("2014-04-04 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(74), Timestamp.valueOf("1940-03-01 00:00:00"), Timestamp.valueOf("2014-11-06 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(74), Timestamp.valueOf("1940-03-01 00:00:00"), Timestamp.valueOf("2014-05-02 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(74), Timestamp.valueOf("1940-03-01 00:00:00"), Timestamp.valueOf("2014-09-26 00:00:00"), None)
    ).toDS()

    // When
    val result = McocePatients.findPatientBirthDate(input)

    // Then
    assertDSs(result, expected)
  }

  "findPatientBirthDateWithNullAge" should "return a Dataset with the null birth date for each patient when patient have no age" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: Dataset[PatientMcoce] = Seq(
      PatientMcoce("200410", 1, None, null, Timestamp.valueOf("2014-04-18 00:00:00"), None)
    ).toDS()

    val expected: Dataset[PatientMcoce] = Seq(
      PatientMcoce("200410", 1, None, null, Timestamp.valueOf("2014-04-18 00:00:00"), None)
    ).toDS()

    // When
    val result = McocePatients.findPatientBirthDate(input)

    // Then
    assertDSs(result, expected)
  }

  "findGender" should "return a Dataset with the correct gender for each patient" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: Dataset[PatientMcoce] = Seq(
      PatientMcoce("200410", 1, Some(79), Timestamp.valueOf("1935-04-01 00:00:00"), Timestamp.valueOf("2014-04-18 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(73), Timestamp.valueOf("1940-03-01 00:00:00"), Timestamp.valueOf("2014-01-09 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(73), Timestamp.valueOf("1940-03-01 00:00:00"), Timestamp.valueOf("2014-02-11 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(74), Timestamp.valueOf("1940-03-01 00:00:00"), Timestamp.valueOf("2014-07-18 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(74), Timestamp.valueOf("1940-03-01 00:00:00"), Timestamp.valueOf("2014-12-12 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(74), Timestamp.valueOf("1940-03-01 00:00:00"), Timestamp.valueOf("2014-04-15 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(74), Timestamp.valueOf("1940-03-01 00:00:00"), Timestamp.valueOf("2014-10-27 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(74), Timestamp.valueOf("1940-03-01 00:00:00"), Timestamp.valueOf("2014-04-04 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(74), Timestamp.valueOf("1940-03-01 00:00:00"), Timestamp.valueOf("2014-11-06 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(74), Timestamp.valueOf("1940-03-01 00:00:00"), Timestamp.valueOf("2014-05-02 00:00:00"), None),
      PatientMcoce("2004100010", 2, Some(74), Timestamp.valueOf("1940-03-01 00:00:00"), Timestamp.valueOf("2014-09-26 00:00:00"), None)
    ).toDS()

    val expected: Dataset[PatientMcoce] = Seq(
      PatientMcoce("200410", 1, Some(79), Timestamp.valueOf("1935-04-01 00:00:00"), Timestamp.valueOf("2014-04-18 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(73), Timestamp.valueOf("1940-03-01 00:00:00"), Timestamp.valueOf("2014-01-09 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(73), Timestamp.valueOf("1940-03-01 00:00:00"), Timestamp.valueOf("2014-02-11 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(74), Timestamp.valueOf("1940-03-01 00:00:00"), Timestamp.valueOf("2014-07-18 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(74), Timestamp.valueOf("1940-03-01 00:00:00"), Timestamp.valueOf("2014-12-12 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(74), Timestamp.valueOf("1940-03-01 00:00:00"), Timestamp.valueOf("2014-04-15 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(74), Timestamp.valueOf("1940-03-01 00:00:00"), Timestamp.valueOf("2014-10-27 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(74), Timestamp.valueOf("1940-03-01 00:00:00"), Timestamp.valueOf("2014-04-04 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(74), Timestamp.valueOf("1940-03-01 00:00:00"), Timestamp.valueOf("2014-11-06 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(74), Timestamp.valueOf("1940-03-01 00:00:00"), Timestamp.valueOf("2014-05-02 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(74), Timestamp.valueOf("1940-03-01 00:00:00"), Timestamp.valueOf("2014-09-26 00:00:00"), None)
    ).toDS()


    // When
    val result = McocePatients.findPatientGender(input)

    // Then
    assertDSs(result, expected)
  }

  "findDeathDate" should "return the same dataset" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: Dataset[PatientMcoce] = Seq(
      PatientMcoce("Patient_01", 2, Some(45), Timestamp.valueOf("1975-01-01 00:00:00"), Timestamp.valueOf("2020-01-01 00:00:00"), None),
      PatientMcoce("Patient_02", 1, Some(50), Timestamp.valueOf("1959-01-01 00:00:00"), Timestamp.valueOf("2010-01-01 00:00:00"), None)
    ).toDS()

    val expected: Dataset[PatientMcoce] = Seq(
      PatientMcoce("Patient_01", 2, Some(45), Timestamp.valueOf("1975-01-01 00:00:00"), Timestamp.valueOf("2020-01-01 00:00:00"), None),
      PatientMcoce("Patient_02", 1, Some(50), Timestamp.valueOf("1959-01-01 00:00:00"), Timestamp.valueOf("2010-01-01 00:00:00"), None)
    ).toDS()

    // When
    val result: Dataset[PatientMcoce] = McocePatients.findPatientDeathDate(input)

    // Then
    assertDSs(result, expected)
  }

  "convert PatientMcocetoPatient" should "return Dataset of Patients" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: Dataset[PatientMcoce] = Seq(
      PatientMcoce("Patient_01", 2, Some(45), Timestamp.valueOf("1975-01-01 00:00:00"), Timestamp.valueOf("2020-01-01 00:00:00"), None),
      PatientMcoce("Patient_02", 1, Some(50), Timestamp.valueOf("1959-01-01 00:00:00"), Timestamp.valueOf("2010-01-01 00:00:00"), None)
    ).toDS()

    val expected: Dataset[Patient] = Seq(
      Patient("Patient_01", 2, Timestamp.valueOf("1975-01-01 00:00:00"), None),
      Patient("Patient_02", 1, Timestamp.valueOf("1959-01-01 00:00:00"), None)
    ).toDS()

    // When
    val result = McocePatients.fromDerivedPatienttoPatient(input)

    // Then
    assertDSs(result, expected)
  }

  "getInput" should "read file" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val mcoce = spark.read.parquet("src/test/resources/test-input/MCO_CE.parquet")
    val sources = Sources(mcoCe = Some(mcoce))

    val expected: Dataset[PatientMcoce] = Seq(
      PatientMcoce("200410", 1, Some(79), null, Timestamp.valueOf("2014-04-18 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(73), null, Timestamp.valueOf("2014-01-09 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(73), null, Timestamp.valueOf("2014-02-11 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(74), null, Timestamp.valueOf("2014-07-18 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(74), null, Timestamp.valueOf("2014-12-12 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(74), null, Timestamp.valueOf("2014-04-15 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(74), null, Timestamp.valueOf("2014-10-27 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(74), null, Timestamp.valueOf("2014-04-04 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(74), null, Timestamp.valueOf("2014-11-06 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(74), null, Timestamp.valueOf("2014-05-02 00:00:00"), None),
      PatientMcoce("2004100010", 1, Some(74), null, Timestamp.valueOf("2014-09-26 00:00:00"), None)
    ).toDS()

    // When
    val result = McocePatients.getInput(sources)

    // Then
    assertDSs(result, expected)
  }

  "extract" should "build patients with actual data" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val mcoce = spark.read.parquet("src/test/resources/test-input/MCO_CE.parquet")
    val sources = Sources(mcoCe = Some(mcoce))

    val expected: Dataset[Patient] = Seq(
      Patient("200410", 1, Timestamp.valueOf("1935-04-01 00:00:00"), None),
      Patient("2004100010", 1, Timestamp.valueOf("1940-03-01 00:00:00"), None)
    ).toDS()

    // When
    val result = McocePatients.extract(sources)

    // Then
    assertDSs(result, expected)
  }
}
