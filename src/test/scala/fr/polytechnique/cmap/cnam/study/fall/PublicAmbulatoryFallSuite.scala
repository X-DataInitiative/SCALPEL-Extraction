package fr.polytechnique.cmap.cnam.study.fall

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{DcirAct, McoCEAct, McoCIM10Act, Outcome}
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class PublicAmbulatoryFallSuite extends SharedContext {

  "isPublicAmbulatory" should "return true for correct events" in {
    // Given
    val event = McoCEAct("georgette", "ACE", "angine", makeTS(2010, 2, 6))

    // When
    val result = PublicAmbulatoryFall.isPublicAmbulatory(event)

    // Then
    assert(result)
  }

  it should "return false for incorrect events" in {
    // Given
    val event = McoCIM10Act("georgette", "ACE", "angine", makeTS(2010, 2, 6))

    // When
    val result = PublicAmbulatoryFall.isPublicAmbulatory(event)

    // Then
    assert(!result)
  }

  "isCorrectCamCode" should "return true for correct events" in {
    // Given
    val event = McoCEAct("georgette", "ACE", "MZMP007", makeTS(2010, 2, 6))

    // When
    val result = PublicAmbulatoryFall.isCorrectCamCode(event)

    // Then
    assert(result)
  }

  "transform" should "return true for correcy events" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val events = List(
      McoCEAct("georgette", "ACE", "MZMP007", makeTS(2010, 2, 6)),
      McoCEAct("george", "ACE", "whatever", makeTS(2010, 2, 6)),
      DcirAct("john", "ACE", "MZMP007", makeTS(2010, 2, 6))
    ).toDS

    val expected = List(
      Outcome("georgette", PublicAmbulatoryFall.outcomeName, makeTS(2010, 2, 6))
    ).toDS

    // When
    val result = PublicAmbulatoryFall.transform(events)

    // Then
    assertDSs(result, expected)
  }

}
