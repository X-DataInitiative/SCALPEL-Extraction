package fr.polytechnique.cmap.cnam.study.fall

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class LiberalFracturesSuite extends SharedContext {

  "transform" should "transform events into outcomes" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    //Given
    val events = Seq(
      MainDiagnosis("Pierre", "3", "S02.35", makeTS(2017, 7, 18)),
      MainDiagnosis("Ben", "3", "S02.35", makeTS(2017, 7, 18)),
      AssociatedDiagnosis("Sam", "3", "S02.35", makeTS(2015, 7, 18))
    ).toDF.as[Event[MedicalAct]]
    val expected = Seq(
      Outcome("Pierre", "Liberal", makeTS(2017, 7, 18)),
      Outcome("Ben", "Liberal", makeTS(2017, 7, 18)),
      Outcome("Sam", "Liberal", makeTS(2015, 7, 18))
    ).toDF.as[Event[Outcome]]

    //When
    val result = LiberalFractures.transform(events)

    //Then
    assertDSs(result, expected)

  }

}
