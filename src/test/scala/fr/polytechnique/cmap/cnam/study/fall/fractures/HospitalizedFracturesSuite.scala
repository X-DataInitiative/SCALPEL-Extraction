// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.fall.fractures

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{Outcome, _}
import fr.polytechnique.cmap.cnam.study.fall.extractors.{Death, Mutation, Transfer}
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
    val result = HospitalizedFractures.filterDiagnosisForFracturesFollowUp(badStays)(input)

    // Then
    assertDSs(result, expected)
  }

  "getFourthLevelSeverity" should "return diagnosis where then patient died at the end of the same hospital stay" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: Dataset[Event[Diagnosis]] = List(
      McoMainDiagnosis("Paul", "1", "hemorroides", makeTS(2017, 7, 20)),
      McoMainDiagnosis("Pierre", "3", "jambe cassée", makeTS(2017, 7, 18)),
      McoMainDiagnosis("Pierre", "4", "jambe cassée", makeTS(2016, 7, 18))
    ).toDS

    val stays: Dataset[Event[HospitalStay]] = List[Event[HospitalStay]](
      McoHospitalStay("Paul", "1", Death.value, 8.0D, makeTS(2017, 7, 20), Some(makeTS(2017, 7, 20))),
      McoHospitalStay("Pierre", "3", Mutation.value, 8.0D, makeTS(2017, 7, 18), Some(makeTS(2017, 7, 18))),
      McoHospitalStay("Pierre", "4", Transfer.value, 8.0D, makeTS(2016, 7, 18), Some(makeTS(2016, 7, 18)))
    ).toDS()

    val expected: Dataset[Event[Diagnosis]] = List(
      McoMainDiagnosis("Paul", "1", "hemorroides", makeTS(2017, 7, 20))
    ).toDS

    // When
    val result = HospitalizedFractures.getFourthLevelSeverity(stays)(input)

    // Then
    assertDSs(result, expected)
  }

  "getThirdLevel" should "return diagnosis where the patient did have a surgery during the same stay" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: Dataset[Event[Diagnosis]] = List(
      McoMainDiagnosis("Paul", "1", "hemorroides", makeTS(2017, 7, 20)),
      McoMainDiagnosis("Pierre", "3", "jambe cassée", makeTS(2017, 7, 18)),
      McoMainDiagnosis("Pierre", "4", "jambe cassée", makeTS(2016, 7, 18))
    ).toDS

    val surgeries: Dataset[Event[MedicalAct]] = List[Event[MedicalAct]](
      McoCCAMAct("Pierre", "5", "jambe", 8.0D, makeTS(2017, 7, 18), Some(makeTS(2017, 7, 18))),
      McoCCAMAct("Pierre", "4", "test", 8.0D, makeTS(2016, 7, 18), Some(makeTS(2016, 7, 18)))
    ).toDS()

    val expected: Dataset[Event[Diagnosis]] = List(
      McoMainDiagnosis("Pierre", "4", "jambe cassée", makeTS(2016, 7, 18))
    ).toDS

    // When
    val result = HospitalizedFractures.getThirdLevelSeverity(surgeries)(input)

    // Then
    assertDSs(result, expected)
  }

  "assignSeverityToDiagnosis" should "assign a weight to a Diagnosis based on stays and surgeries of the patient" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: Dataset[Event[Diagnosis]] = List(
      McoMainDiagnosis("Paul", "1", "ColDuFemur", makeTS(2017, 7, 20)),
      McoMainDiagnosis("Pierre", "3", "Coude", makeTS(2017, 7, 18)),
      McoMainDiagnosis("Pierre", "4", "Poignet", makeTS(2016, 7, 18))
    ).toDS

    val surgeries: Dataset[Event[MedicalAct]] = List[Event[MedicalAct]](
      McoCCAMAct("Pierre", "4", "test", 8.0D, makeTS(2016, 7, 18), None)
    ).toDS()

    val stays: Dataset[Event[HospitalStay]] = List[Event[HospitalStay]](
      McoHospitalStay("Paul", "1", Death.value, 8.0D, makeTS(2017, 7, 20), Some(makeTS(2017, 7, 20)))
    ).toDS()

    val expected: Dataset[Event[Diagnosis]] = List(
      McoMainDiagnosis("Paul", "1", "ColDuFemur", 4D, makeTS(2017, 7, 20), None),
      McoMainDiagnosis("Pierre", "3", "Coude", 2D, makeTS(2017, 7, 18)),
      McoMainDiagnosis("Pierre", "4", "Poignet", 3D, makeTS(2016, 7, 18))
    ).toDS

    // When
    val result = HospitalizedFractures.assignSeverityToDiagnosis(stays, surgeries)(input)

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
      McoMainDiagnosis("Charlotte", "9", "S02.42", makeTS(2017, 10, 22)),
      McoAssociatedDiagnosis("Jacques", "8", "qu'est-ce-que tu fais là?", makeTS(2017, 7, 18))
    ).toDS

    val surgeries: Dataset[Event[MedicalAct]] = List[Event[MedicalAct]](
      McoCCAMAct("Pierre", "3", "test", 8.0D, makeTS(2016, 7, 18), None)
    ).toDS()

    val stays: Dataset[Event[HospitalStay]] = List[Event[HospitalStay]](
      McoHospitalStay("Paul", "7", Death.value, 8.0D, makeTS(2017, 1, 2), Some(makeTS(2017, 1, 3)))
    ).toDS()

    val medicalActs = Seq(
      McoCCAMAct("Paul", "1", "LJGA001", makeTS(2017, 7, 20))
    ).toDS

    val expected: Dataset[Event[Outcome]] = Seq[Event[Outcome]](
      Outcome("Pierre", "AllSites", "hospitalized_fall", 3D, makeTS(2017, 7, 18), None),
      Outcome("Paul", "AllSites", "hospitalized_fall", 4D, makeTS(2017, 1, 2), None),
      Outcome("Charlotte", "AllSites", "hospitalized_fall", 2D, makeTS(2017, 10, 22), None)
    ).toDS

    // When
    val result = HospitalizedFractures.transform(diagnoses, medicalActs, stays, surgeries, List(AllSites))
    // Then
    assertDSs(result, expected)
  }
}
