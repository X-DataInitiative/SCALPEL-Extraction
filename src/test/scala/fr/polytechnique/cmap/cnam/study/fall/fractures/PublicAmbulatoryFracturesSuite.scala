// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.fall.fractures

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{DcirAct, Event, McoCeCcamAct, McoCIM10Act, Outcome}
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class PublicAmbulatoryFracturesSuite extends SharedContext {

  "isPublicAmbulatory" should "return true for correct events" in {
    // Given
    val event = McoCeCcamAct("georgette", "ACE", "angine", makeTS(2010, 2, 6))

    // When
    val result = PublicAmbulatoryFractures.isPublicAmbulatory(event)

    // Then
    assert(result)
  }

  it should "return false for incorrect events" in {
    // Given
    val event = McoCIM10Act("georgette", "ACE", "angine", makeTS(2010, 2, 6))

    // When
    val result = PublicAmbulatoryFractures.isPublicAmbulatory(event)

    // Then
    assert(!result)
  }

  "containsNonHospitalizedCcam" should "return true for correct events" in {
    // Given
    val event = McoCeCcamAct("georgette", "ACE", "MZMP007", makeTS(2010, 2, 6))

    // When
    val result = PublicAmbulatoryFractures.containsNonHospitalizedCcam(event)

    // Then
    assert(result)
  }

  "transform" should "return true for correcy events" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val events = Seq(
      McoCeCcamAct("georgette", "ACE", "MZMP007", makeTS(2010, 2, 6)),
      McoCeCcamAct("george", "ACE", "whatever", makeTS(2010, 2, 6)),
      DcirAct("john", "ACE", "MZMP007", makeTS(2010, 2, 6))
    ).toDS

    val expected: Dataset[Event[Outcome]] = Seq[Event[Outcome]](
      Outcome("georgette", "MembreSuperieurDistal", PublicAmbulatoryFractures.outcomeName, 1D, makeTS(2010, 2, 6), None)
    ).toDS

    // When
    val result = PublicAmbulatoryFractures.transform(events)

    // Then
    assertDSs(result, expected)
  }

}
