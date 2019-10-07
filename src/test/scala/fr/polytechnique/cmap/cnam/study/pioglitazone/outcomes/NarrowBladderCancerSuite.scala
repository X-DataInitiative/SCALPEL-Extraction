// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.pioglitazone.outcomes

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.util._
import fr.polytechnique.cmap.cnam.util.functions._

class NarrowBladderCancerSuite extends SharedContext {

  import datetime.implicits._

  private val patientID = "SomePatient"
  private val hospitalStayID = "SomeStay"
  private val dcirGroupID = DcirAct.category
  private val date = makeTS(2010, 1, 1)

  "checkDates" should "return true if all the events have the same date, or if the list is empty" in {
    // Given
    val events: List[Event[AnyEvent]] = List(
      MainDiagnosis(patientID, hospitalStayID, "C67", date),
      LinkedDiagnosis(patientID, hospitalStayID, "C67", date),
      AssociatedDiagnosis(patientID, hospitalStayID, "C67", date),
      McoCCAMAct(patientID, hospitalStayID, "A", date)
    )

    // When|Then
    assert(NarrowBladderCancer.checkDates(events))
    assert(NarrowBladderCancer.checkDates(Nil))
  }

  it should "return false otherwise" in {
    // Given
    val events: List[Event[AnyEvent]] = List(
      AssociatedDiagnosis(patientID, hospitalStayID, "C67", date),
      McoCCAMAct(patientID, hospitalStayID, "A", date + 1.day)
    )

    // When|Then
    assert(!NarrowBladderCancer.checkDates(events))
  }

  "checkDiagnosesInStay" should "return true if the diagnoses in a stay conform to the rules" in {
    // Given
    val dpEvent: List[Event[AnyEvent]] = List(
      MainDiagnosis(patientID, hospitalStayID, "C67", date), // true
      MainDiagnosis(patientID, hospitalStayID, "AAA", date) // false
    )
    val drEvent: List[Event[AnyEvent]] = List(
      LinkedDiagnosis(patientID, hospitalStayID, "C67", date), // true
      LinkedDiagnosis(patientID, hospitalStayID, "AAA", date) // false
    )
    val dasWithDp: List[Event[AnyEvent]] = List(
      AssociatedDiagnosis(patientID, hospitalStayID, "C67", date),
      MainDiagnosis(patientID, hospitalStayID, "C77", date)
    )
    val dasWithDr: List[Event[AnyEvent]] = List(
      AssociatedDiagnosis(patientID, hospitalStayID, "C67", date),
      LinkedDiagnosis(patientID, hospitalStayID, "C78", date)
    )

    // When|Then
    assert(NarrowBladderCancer.checkDiagnosesInStay(dpEvent))
    assert(NarrowBladderCancer.checkDiagnosesInStay(drEvent))
    assert(NarrowBladderCancer.checkDiagnosesInStay(dasWithDp))
    assert(NarrowBladderCancer.checkDiagnosesInStay(dasWithDr))
  }

  it should "return false otherwise" in {
    // Given
    val emptyList: List[Event[AnyEvent]] = Nil
    val wrongCodeEvents: List[Event[AnyEvent]] = List(
      MainDiagnosis(patientID, hospitalStayID, "AAA", date),
      LinkedDiagnosis(patientID, hospitalStayID, "AAA", date),
      AssociatedDiagnosis(patientID, hospitalStayID, "AAA", date)
    )
    val onlyDas: List[Event[AnyEvent]] = List(
      AssociatedDiagnosis(patientID, hospitalStayID, "C67", date)
    )
    val noDas: List[Event[AnyEvent]] = List(
      MainDiagnosis(patientID, hospitalStayID, "C77", date),
      LinkedDiagnosis(patientID, hospitalStayID, "C79", date)
    )

    // When|Then
    assert(!NarrowBladderCancer.checkDiagnosesInStay(wrongCodeEvents))
    assert(!NarrowBladderCancer.checkDiagnosesInStay(onlyDas))
    assert(!NarrowBladderCancer.checkDiagnosesInStay(noDas))
    assert(!NarrowBladderCancer.checkDiagnosesInStay(emptyList))
  }

  "checkMcoActsInStay" should "return true if the mco acts in a stay conform to the rules" in {
    // Given
    val cimAct: List[Event[AnyEvent]] = List(
      McoCIM10Act(patientID, hospitalStayID, "Z510", date), // true
      McoCIM10Act(patientID, hospitalStayID, "AAAA", date) // false
    )
    val camAct: List[Event[AnyEvent]] = List(
      McoCCAMAct(patientID, hospitalStayID, "JDFA001", date), // true
      McoCCAMAct(patientID, hospitalStayID, "AAAAAAA", date) // false
    )

    // When|Then
    assert(NarrowBladderCancer.checkMcoActsInStay(cimAct))
    assert(NarrowBladderCancer.checkMcoActsInStay(camAct))
  }

  it should "return false otherwise" in {
    // Given
    val emptyEvents: List[Event[AnyEvent]] = Nil
    val wrongCodes: List[Event[AnyEvent]] = List(
      McoCCAMAct(patientID, hospitalStayID, "AAAAAAA", date),
      McoCIM10Act(patientID, hospitalStayID, "AAAA", date)
    )

    // When|Then
    assert(!NarrowBladderCancer.checkMcoActsInStay(wrongCodes))
    assert(!NarrowBladderCancer.checkMcoActsInStay(emptyEvents))
  }

  "checkDcirActs" should "check if DCIR acts conform to the rule" in {
    // Given
    val events: List[Event[AnyEvent]] = List(
      DcirAct(patientID, dcirGroupID, "YYYY045", date - 5.month), // false
      DcirAct(patientID, dcirGroupID, "YYYY045", date + 1.month), // true
      DcirAct(patientID, dcirGroupID, "AAAAAAA", date + 1.month) // false
    )

    // When|Then
    assert(NarrowBladderCancer.checkDcirActs(events, date))
  }

  it should "return false otherwise" in {
    // Given
    val emptyDcirEvents: List[Event[AnyEvent]] = Nil
    val wrongDcirEvents: List[Event[AnyEvent]] = List(
      DcirAct(patientID, dcirGroupID, "AAAAAAA", date),
      DcirAct(patientID, dcirGroupID, "YYYY045", date + 5.months),
      DcirAct(patientID, dcirGroupID, "YYYY045", date - 5.months)
    )

    // When|Then
    assert(!NarrowBladderCancer.checkDcirActs(emptyDcirEvents, date))
    assert(!NarrowBladderCancer.checkDcirActs(wrongDcirEvents, date))
  }

  "checkHospitalStay" should "return true if there is a valid outcome in the given events or false otherwise" in {
    // Given
    val dcirActs: List[Event[AnyEvent]] = List(
      DcirAct(patientID, dcirGroupID, "YYYY045", date + 1.month)
    )

    val mcoDiagnoses: List[Event[AnyEvent]] = List(
      AssociatedDiagnosis(patientID, hospitalStayID, "C67", date),
      LinkedDiagnosis(patientID, hospitalStayID, "C79", date)
    )

    val mcoActs: List[Event[AnyEvent]] = List(
      McoCIM10Act(patientID, hospitalStayID, "Z510", date)
    )

    // When|Then
    // true
    assert(NarrowBladderCancer.checkHospitalStay(mcoDiagnoses ++ mcoActs, Nil))
    assert(NarrowBladderCancer.checkHospitalStay(mcoDiagnoses, dcirActs))
    // false
    assert(!NarrowBladderCancer.checkHospitalStay(Nil, Nil))
    assert(!NarrowBladderCancer.checkHospitalStay(Nil, dcirActs))
    assert(!NarrowBladderCancer.checkHospitalStay(mcoDiagnoses, Nil))
    assert(!NarrowBladderCancer.checkHospitalStay(mcoActs, Nil))
    assert(!NarrowBladderCancer.checkHospitalStay(mcoActs, dcirActs))
  }

  "findOutcomes" should "return all outcomes found in the list of a given patient's events" in {
    // Given
    val input: Iterator[Event[AnyEvent]] = Iterator(
      // dcir
      DcirAct(patientID, dcirGroupID, "YYYY045", date + 1.month),
      // hosp1 (true: dcir act)
      AssociatedDiagnosis(patientID, "hosp1", "C67", date),
      LinkedDiagnosis(patientID, "hosp1", "C79", date),
      // hosp2 (true: mco z510)
      MainDiagnosis(patientID, "hosp2", "C67", date + 6.months),
      McoCIM10Act(patientID, "hosp2", "Z510", date + 6.months),
      // hosp3 (false)
      LinkedDiagnosis(patientID, "hosp3", "C67", date + 9.months)
    )
    val expected: Seq[Event[Outcome]] = Stream(
      Outcome(patientID, NarrowBladderCancer.outcomeName, date + 6.months),
      Outcome(patientID, NarrowBladderCancer.outcomeName, date)
    )

    // When
    val result = NarrowBladderCancer.findOutcomes(input)

    // Then
    assert(result == expected)
  }

  "transform" should "return all outcomes given the datasets of diagnoses and medical acts" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val diagnoses: Dataset[Event[Diagnosis]] = Seq(
      AssociatedDiagnosis("PatientA", "hosp1", "C67", date),
      LinkedDiagnosis("PatientA", "hosp1", "C79", date),
      MainDiagnosis("PatientA", "hosp2", "C67", date + 4.months),
      MainDiagnosis("PatientA", "hosp3", "C67", date + 6.months),
      LinkedDiagnosis("PatientB", "hosp4", "C67", date + 8.months),
      MainDiagnosis("PatientC", "hosp5", "C67", date + 10.months)
    ).toDS

    val acts: Dataset[Event[MedicalAct]] = Seq(
      DcirAct("PatientA", dcirGroupID, "YYYY045", date + 1.month),
      McoCIM10Act("PatientA", "hosp2", "Z510", date + 4.months),
      McoCCAMAct("PatientB", "hosp4", "JDFA001", date + 8.months)
    ).toDS

    val expected: Dataset[Event[Outcome]] = Seq(
      Outcome("PatientA", NarrowBladderCancer.outcomeName, date),
      Outcome("PatientA", NarrowBladderCancer.outcomeName, date + 4.months),
      Outcome("PatientB", NarrowBladderCancer.outcomeName, date + 8.months)
    ).toDS

    // When
    val result = NarrowBladderCancer.transform(diagnoses, acts)

    // Then
    assertDFs(result.toDF, expected.toDF)
  }
}
