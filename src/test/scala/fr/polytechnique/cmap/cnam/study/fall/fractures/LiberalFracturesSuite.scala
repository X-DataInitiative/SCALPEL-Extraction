// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.fall.fractures

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class LiberalFracturesSuite extends SharedContext {

  "transform" should "transform events into outcomes" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    //Given
    val events = Seq(
      DcirAct("Pierre", "3", "MADP001", makeTS(2017, 7, 18)),
      DcirAct("Ben", "3", "MZMP007", makeTS(2017, 7, 18)),
      DcirAct("Sam", "3", "HBED009", makeTS(2015, 7, 18)),
      DcirAct("Sam", "3", "4561", makeTS(2015, 7, 18))
    ).toDF.as[Event[MedicalAct]]
    val expected = Seq(
      Outcome("Pierre", "Clavicule", "Liberal", 1D, makeTS(2017, 7, 18), None),
      Outcome("Ben", "MembreSuperieurDistal", "Liberal", 1D, makeTS(2017, 7, 18), None),
      Outcome("Sam", "CraneFace", "Liberal", 1D, makeTS(2015, 7, 18), None),
      Outcome("Sam", "undefined", "Liberal", 1D, makeTS(2015, 7, 18), None)
    ).toDF.as[Event[Outcome]]

    //When
    val result = LiberalFractures.transform(events)

    //Then
    assertDSs(result, expected)

  }

}
