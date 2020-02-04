// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.tracklosses

import org.apache.spark.sql.{DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{Event, Molecule, Trackloss}
import fr.polytechnique.cmap.cnam.util.functions

class TracklossTransformerSuite extends SharedContext {

  "transform" should "return correct result" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    // Given
    val config = TracklossesConfig(functions.makeTS(2006, 12, 31))
    val drugs: Dataset[Event[Molecule]] = Seq(
      Molecule("Patient_01", "3400935418487", 1.0, functions.makeTS(2006, 1, 15)),
      Molecule("Patient_01", "3400935418487", 1.0, functions.makeTS(2006, 6, 30)),
      Molecule("Patient_02", "3400935563538", 1.0, functions.makeTS(2006, 1, 5)),
      Molecule("Patient_02", "3400935563538", 1.0, functions.makeTS(2006, 1, 15)),
      Molecule("Patient_02", "3400935563538", 1.0, functions.makeTS(2006, 1, 30)),
      Molecule("Patient_02", "3400935563538", 1.0, functions.makeTS(2006, 1, 30))
    ).toDS()
    val expected: Dataset[Event[Trackloss]] = Seq(
      Trackloss("Patient_01", functions.makeTS(2006, 3, 15)),
      Trackloss("Patient_01", functions.makeTS(2006, 8, 30)),
      Trackloss("Patient_02", functions.makeTS(2006, 3, 30))
    ).toDS()
    //when
    val res = new TracklossTransformer(config).transform(drugs)
    //then
    assertDSs(expected, res)
  }
}
