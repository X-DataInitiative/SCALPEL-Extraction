// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.fall.fractures

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{Outcome, _}
import fr.polytechnique.cmap.cnam.util.functions._


class HospitalizedFracturesSuite extends SharedContext {

  "filterDiagnosesWithoutDP" should
    "filter out LinkedDiagnosis and AssociatedDiagnoses that has not a MainDiagnosis" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val diagnoses: Dataset[Event[Diagnosis]] = Seq(
      McoMainDiagnosis("Pierre", "3", "S02.42", makeTS(2017, 7, 18)),
      McoMainDiagnosis("Paul", "1", "S42.54678", makeTS(2017, 7, 20)),
      McoMainDiagnosis("Paul", "7", "S02.42", makeTS(2017, 1, 2)),
      McoAssociatedDiagnosis("Jacques", "8", "qu'est-ce-que tu fais là?", makeTS(2017, 7, 18)),
      McoLinkedDiagnosis("Jacques", "8", "qu'est-ce-que tu fais là?", makeTS(2017, 7, 18)),
      McoAssociatedDiagnosis("Paul", "7", "S02.42", makeTS(2017, 1, 2)),
      McoLinkedDiagnosis("Pierre", "3", "S02.42", makeTS(2017, 7, 18))
    ).toDS

    val expected: Dataset[Event[Diagnosis]] = Seq(
      McoMainDiagnosis("Pierre", "3", "S02.42", makeTS(2017, 7, 18)),
      McoMainDiagnosis("Paul", "1", "S42.54678", makeTS(2017, 7, 20)),
      McoMainDiagnosis("Paul", "7", "S02.42", makeTS(2017, 1, 2)),
      McoAssociatedDiagnosis("Paul", "7", "S02.42", makeTS(2017, 1, 2)),
      McoLinkedDiagnosis("Pierre", "3", "S02.42", makeTS(2017, 7, 18))
    ).toDS

    // When
    val result = HospitalizedFractures.filterDiagnosesWithoutDP(diagnoses)
    // Then
    assertDSs(result, expected)
  }

  "getFractureFollowUpStays" should "get the HospitalStay where the CCAM is in CCAMExceptions" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val medicalActs: Dataset[Event[MedicalAct]] = Seq(
      McoCCAMAct("Paul", "1", "LJGA001", makeTS(2017, 7, 20)),
      McoCCAMAct("Paul", "1", "Whatever", makeTS(2017, 12, 20))
    ).toDS

    val expected: Dataset[HospitalStayID] = Seq(
      HospitalStayID("Paul", "1")
    ).toDS

    // When
    val result = HospitalizedFractures.getFractureFollowUpStays(medicalActs)
    // Then
    assertDSs(result, expected)
  }
  "filterDiagnosisForFracturesFollowUp" should
    "return Diagnosis which has not a fracture followup hospital stay" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: Dataset[Event[Diagnosis]] = List(
      McoMainDiagnosis("Paul", "1", "hemorroides", makeTS(2017, 7, 20)),
      McoMainDiagnosis("Pierre", "3", "jambe cassée", makeTS(2017, 7, 18)),
      McoMainDiagnosis("Pierre", "4", "jambe cassée", makeTS(2016, 7, 18))
    ).toDS

    val badStays = Seq(
      HospitalStayID("Pierre", "3")
    ).toDS

    val expected: Dataset[Event[Diagnosis]] = List(
      McoMainDiagnosis("Paul", "1", "hemorroides", makeTS(2017, 7, 20)),
      McoMainDiagnosis("Pierre", "4", "jambe cassée", makeTS(2016, 7, 18))
    ).toDS

    // When
    val result = HospitalizedFractures.filterDiagnosisForFracturesFollowUp(input, badStays)

    // Then
    assertDSs(result, expected)
  }

  "transform" should "return Fractures Event Dataset based on the algorithm" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val diagnoses = Seq(
      McoMainDiagnosis("Pierre", "3", "S02.42", makeTS(2017, 7, 18)),
      McoMainDiagnosis("Paul", "1", "S42.54678", makeTS(2017, 7, 20)),
      McoMainDiagnosis("Paul", "7", "S02.42", makeTS(2017, 1, 2)),
      McoAssociatedDiagnosis("Jacques", "8", "qu'est-ce-que tu fais là?", makeTS(2017, 7, 18))
    ).toDS

    val medicalActs = Seq(
      McoCCAMAct("Paul", "1", "LJGA001", makeTS(2017, 7, 20))
    ).toDS

    val expected = Seq(
      Outcome("Pierre", "AllSites", "hospitalized_fall", makeTS(2017, 7, 18)),
      Outcome("Paul", "AllSites", "hospitalized_fall", makeTS(2017, 1, 2))
    ).toDS

    // When
    val result = HospitalizedFractures.transform(diagnoses, medicalActs, List(AllSites))
    // Then
    assertDSs(result, expected)
  }

  "transform" should "return correct weight" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val diagnoses = Seq(
      McoMainDiagnosis("Pierre", "3", "S02.42", 2.0, makeTS(2017, 7, 18)),
      McoMainDiagnosis("Jean", "2", "S02.42", 3.0, makeTS(2017, 7, 18)),
      McoMainDiagnosis("Kevin", "4", "S02.42", 4.0, makeTS(2017, 7, 18)),
      McoMainDiagnosis("Paul", "1", "S42.54678", 2.0, makeTS(2017, 7, 20)),
      McoMainDiagnosis("Paul", "7", "S42.54678", 2.0, makeTS(2017, 1, 2)),
      McoAssociatedDiagnosis("Jacques", "8", "qu'est-ce-que tu fais là?", 2.0, makeTS(2017, 7, 18))
    ).toDS

    val medicalActs = Seq(
      McoCCAMAct("Paul", "1", "LJGA001", makeTS(2017, 7, 20))
    ).toDS

    val expected = Seq(
      Outcome("Pierre", "AllSites", "hospitalized_fall", 2.0, makeTS(2017, 7, 18)),
      Outcome("Jean", "AllSites", "hospitalized_fall", 3.0, makeTS(2017, 7, 18)),
      Outcome("Kevin", "AllSites", "hospitalized_fall", 4.0, makeTS(2017, 7, 18)),
      Outcome("Paul", "AllSites", "hospitalized_fall", 2.0, makeTS(2017, 1, 2))
    ).toDS

    // When
    val result = HospitalizedFractures.transform(diagnoses, medicalActs, List(AllSites))
    // Then
    assertDSs(result, expected)
  }
}
