package fr.polytechnique.cmap.cnam.study.pioglitazone

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{Diagnosis, Event, Outcome}
import fr.polytechnique.cmap.cnam.study.pioglitazone.outcomes.BroadBladderCancer

class BroadBladderCancerSuite extends SharedContext {

  "isDirectDiagnosis" should "filter events when either DP or DR equals C67" in {

    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    import fr.polytechnique.cmap.cnam.util.functions.makeTS

    val inputDS = Seq(
      Event[Diagnosis]("Patient1", "main_diagnosis", "g1", "C67", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20))),
      Event[Diagnosis]("Patient2", "main_diagnosis", "g2", "C67", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20))),
      Event[Diagnosis]("Patient3", "linked_diagnosis", "g3", "C67", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20))),
      Event[Diagnosis]("Patient4", "associated_diagnosis", "g4", "C67", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20))),
      Event[Diagnosis]("Patient4", "linked_diagnosis", "g4", "C77", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20))),
      Event[Diagnosis]("Patient5", "associated_diagnosis", "g5", "C67", 1.0, makeTS(2006, 2, 20), Some(makeTS(2006, 2, 20))),
      Event[Diagnosis]("Patient5", "main_diagnosis", "g5", "C79", 1.0, makeTS(2006, 2, 20), Some(makeTS(2006, 2, 20))),
      Event[Diagnosis]("Patient6", "associated_diagnosis", "g6", "C67", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20))),
      Event[Diagnosis]("Patient6", "linked_diagnosis", "g7", "C78", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20))),
      Event[Diagnosis]("Patient8", "linked_diagnosis", "g8", "C78", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20))),
      Event[Diagnosis]("Patient9", "main_diagnosis", "g9", "C79", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20))),
      Event[Diagnosis]("Patient10", "ssr_main_diagnosis", "g10", "C67", 1.0, makeTS(2006, 10, 20), Some(makeTS(2006, 10, 20))),
      Event[Diagnosis]("Patient11", "ssr_etiologic_diagnosis", "g10", "C67", 1.0, makeTS(2006, 10, 20), Some(makeTS(2006, 10, 20))),
      Event[Diagnosis]("Patient12", "had_main_diagnosis", "g10", "C67", 1.0, makeTS(2006, 10, 20), Some(makeTS(2006, 10, 20))),
      Event[Diagnosis]("Patient13", "ssr_etiologic_diagnosis", "g10", "C77", 1.0, makeTS(2006, 5, 20), Some(makeTS(2006, 5, 20)))
    ).toDS

    val expectedDS = Seq(
      Event[Diagnosis]("Patient1", "main_diagnosis", "g1", "C67", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20))),
      Event[Diagnosis]("Patient2", "main_diagnosis", "g2", "C67", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20))),
      Event[Diagnosis]("Patient3", "linked_diagnosis", "g3", "C67", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20))),
      Event[Diagnosis]("Patient10", "ssr_main_diagnosis", "g10", "C67", 1.0, makeTS(2006, 10, 20), Some(makeTS(2006, 10, 20))),
      Event[Diagnosis]("Patient11", "ssr_etiologic_diagnosis", "g10", "C67", 1.0, makeTS(2006, 10, 20), Some(makeTS(2006, 10, 20))),
      Event[Diagnosis]("Patient12", "had_main_diagnosis", "g10", "C67", 1.0, makeTS(2006, 10, 20), Some(makeTS(2006, 10, 20)))
    ).toDS

    // When
    import fr.polytechnique.cmap.cnam.study.pioglitazone.outcomes.BroadBladderCancer.isDirectDiagnosis
    val result = inputDS.filter(ev => isDirectDiagnosis(ev))

    // Then
    assertDSs(result, expectedDS)
  }

  "directOutcome" should "map Dataset[Event[Diagnosis]] to Dataset[Event[Outcome]]" in {

    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    import fr.polytechnique.cmap.cnam.util.functions.makeTS

    val inputDS = Seq(
      Event[Diagnosis]("Patient1", "main_diagnosis", "g1", "C67", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20))),
      Event[Diagnosis]("Patient2", "main_diagnosis", "g2", "C67", 1.0, makeTS(2006, 2, 20), Some(makeTS(2006, 2, 20))),
      Event[Diagnosis]("Patient3", "linked_diagnosis", "g3", "C67", 1.0, makeTS(2006, 3, 20), Some(makeTS(2006, 3, 20)))
    ).toDS

    val expectedDS = Seq(
      Outcome("Patient1", "broad_bladder_cancer", makeTS(2006, 1, 20)),
      Outcome("Patient2", "broad_bladder_cancer", makeTS(2006, 2, 20)),
      Outcome("Patient3", "broad_bladder_cancer", makeTS(2006, 3, 20))
    ).toDS

    // When
    import fr.polytechnique.cmap.cnam.study.pioglitazone.outcomes.BroadBladderCancer.BroadBladderCancerOutcome
    val result = inputDS.directOutcomes

    // Then
    assertDSs(result, expectedDS)
  }

  "isGroupDiagnosis" should "return true when DAS equals C67 and either DP or DR equals one of " +
      "(C77, C78, C79) with the same group id" in {

    // Given
    import fr.polytechnique.cmap.cnam.util.functions.makeTS
    val input1 = Seq(
      Event[Diagnosis]("Patient4", "associated_diagnosis", "g4", "C67", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20))),
      Event[Diagnosis]("Patient4", "linked_diagnosis", "g4", "C77", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20)))
    )
    val input2 = Seq(
      Event[Diagnosis]("Patient4", "main_diagnosis", "g5", "C78", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20))),
      Event[Diagnosis]("Patient4", "associated_diagnosis", "g5", "C67", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20)))
    )
    val input3 = Seq(
      Event[Diagnosis]("Patient4", "associated_diagnosis", "g6", "C67", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20))),
      Event[Diagnosis]("Patient4", "main_diagnosis", "g6", "C79", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20)))
    )

    // When
    val result1 = BroadBladderCancer.isGroupDiagnosis(input1)
    val result2 = BroadBladderCancer.isGroupDiagnosis(input2)
    val result3 = BroadBladderCancer.isGroupDiagnosis(input3)

    // Then
    assert(result1)
    assert(result2)
    assert(result3)
  }

  it should "return false when the conditions are not met" in {

    // Given
    import fr.polytechnique.cmap.cnam.util.functions.makeTS
    val input1 = Seq(
      Event[Diagnosis]("Patient4", "associated_diagnosis", "g4", "C67", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20))),
      Event[Diagnosis]("Patient4", "linked_diagnosis", "g4", "C67", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20)))
    )
    val input2 = Seq(
      Event[Diagnosis]("Patient5", "main_diagnosis", "g5", "C67", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20))),
      Event[Diagnosis]("Patient5", "associated_diagnosis", "g5", "C67", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20)))
    )
    val input3 = Seq(
      Event[Diagnosis]("Patient6", "associated_diagnosis", "g6", "C67", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20))),
      Event[Diagnosis]("Patient6", "associated_diagnosis", "g6", "C79", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20)))
    )

    // When
    val result1 = BroadBladderCancer.isGroupDiagnosis(input1)
    val result2 = BroadBladderCancer.isGroupDiagnosis(input2)
    val result3 = BroadBladderCancer.isGroupDiagnosis(input3)

    // Then
    assert(!result1)
    assert(!result2)
    assert(!result3)
  }

  "groupDiagnosis" should "group events based on groupId and tranform each group into Event[Outcome], " +
      "if isGroupDiagnosis returns true" in {

    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    import fr.polytechnique.cmap.cnam.util.functions.makeTS

    val inputDS = Seq(
      Event[Diagnosis]("Patient1", "main_diagnosis", "g1", "C67", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20))),
      Event[Diagnosis]("Patient2", "main_diagnosis", "g2", "C67", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20))),
      Event[Diagnosis]("Patient3", "linked_diagnosis", "g3", "C67", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20))),
      Event[Diagnosis]("Patient4", "associated_diagnosis", "g4", "C67", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20))),
      Event[Diagnosis]("Patient4", "linked_diagnosis", "g4", "C77", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20))),
      Event[Diagnosis]("Patient5", "associated_diagnosis", "g5", "C67", 1.0, makeTS(2006, 2, 20), Some(makeTS(2006, 2, 20))),
      Event[Diagnosis]("Patient5", "main_diagnosis", "g5", "C79", 1.0, makeTS(2006, 2, 20), Some(makeTS(2006, 2, 20))),
      Event[Diagnosis]("Patient6", "associated_diagnosis", "g6", "C67", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20))),
      Event[Diagnosis]("Patient6", "linked_diagnosis", "g7", "C78", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20))),
      Event[Diagnosis]("Patient8", "linked_diagnosis", "g8", "C78", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20))),
      Event[Diagnosis]("Patient1", "main_diagnosis", "g9", "C79", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20)))
    ).toDS

    val expectedDS = Seq(
      Outcome("Patient4", "broad_bladder_cancer", makeTS(2006, 1, 20)),
      Outcome("Patient5", "broad_bladder_cancer", makeTS(2006, 2, 20))
    ).toDS

    // When
    import BroadBladderCancer.BroadBladderCancerOutcome
    val result = inputDS.groupOutcomes

    // Then
    assertDSs(result, expectedDS)
  }

  "transform" should "correctly transform the input Dataset according to BroadBladderCancer definition" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    import fr.polytechnique.cmap.cnam.util.functions.makeTS
    val inputDS = Seq(
      Event[Diagnosis]("Patient1", "main_diagnosis", "g1", "C67", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20))),
      Event[Diagnosis]("Patient2", "main_diagnosis", "g2", "C67", 1.0, makeTS(2006, 2, 20), Some(makeTS(2006, 2, 20))),
      Event[Diagnosis]("Patient3", "linked_diagnosis", "g3", "C67", 1.0, makeTS(2006, 3, 20), Some(makeTS(2006, 3, 20))),
      Event[Diagnosis]("Patient4", "associated_diagnosis", "g4", "C67", 1.0, makeTS(2006, 4, 20), Some(makeTS(2006, 4, 20))),
      Event[Diagnosis]("Patient4", "linked_diagnosis", "g4", "C77", 1.0, makeTS(2006, 4, 20), Some(makeTS(2006, 4, 20))),
      Event[Diagnosis]("Patient5", "associated_diagnosis", "g5", "C67", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20))),
      Event[Diagnosis]("Patient5", "main_diagnosis", "g5", "C79", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20))),
      Event[Diagnosis]("Patient6", "associated_diagnosis", "g6", "C67", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20))),
      Event[Diagnosis]("Patient6", "linked_diagnosis", "g7", "C78", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20))),
      Event[Diagnosis]("Patient8", "linked_diagnosis", "g8", "C78", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20))),
      Event[Diagnosis]("Patient1", "main_diagnosis", "g9", "C79", 1.0, makeTS(2006, 1, 20), Some(makeTS(2006, 1, 20))),
      Event[Diagnosis]("Patient10", "ssr_main_diagnosis", "g10", "C67", 1.0, makeTS(2006, 10, 20), Some(makeTS(2006, 10, 20))),
      Event[Diagnosis]("Patient11", "ssr_etiologic_diagnosis", "g11", "C77", 1.0, makeTS(2006, 11, 20), Some(makeTS(2006, 11, 20))),
      Event[Diagnosis]("Patient12", "had_main_diagnosis", "g12", "C67", 1.0, makeTS(2006, 12, 20), Some(makeTS(2006, 12, 20)))
    ).toDS

    val expectedDS = Seq(
      Outcome("Patient1", "broad_bladder_cancer", makeTS(2006, 1, 20)),
      Outcome("Patient2", "broad_bladder_cancer", makeTS(2006, 2, 20)),
      Outcome("Patient3", "broad_bladder_cancer", makeTS(2006, 3, 20)),
      Outcome("Patient4", "broad_bladder_cancer", makeTS(2006, 4, 20)),
      Outcome("Patient5", "broad_bladder_cancer", makeTS(2006, 1, 20)),
      Outcome("Patient10", "broad_bladder_cancer", makeTS(2006, 10, 20)),
      Outcome("Patient12", "broad_bladder_cancer", makeTS(2006, 12, 20))
    ).toDS

    // When
    val result = BroadBladderCancer.transform(inputDS)

    // Then
    assertDSs(result, expectedDS)
  }

}
