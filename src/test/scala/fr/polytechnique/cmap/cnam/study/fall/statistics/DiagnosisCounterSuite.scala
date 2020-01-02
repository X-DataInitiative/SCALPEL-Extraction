package fr.polytechnique.cmap.cnam.study.fall.statistics

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class DiagnosisCounterSuite extends SharedContext {

  it should "count fractures by DP, DA, DR" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val input = Seq[Event[Diagnosis]](
      McoMainDiagnosis("Patient_02", "10000123_20000123_2007", "C670", makeTS(2007, 1, 29)),
      McoMainDiagnosis("Patient_02", "10000123_20000345_2007", "C671", makeTS(2007, 1, 29)),
      McoMainDiagnosis("Patient_02", "10000123_10000987_2006", "C670", makeTS(2005, 12, 29)),
      McoMainDiagnosis("Patient_02", "10000123_10000543_2006", "C671", makeTS(2005, 12, 24)),
      McoMainDiagnosis("Patient_02", "10000123_30000546_2008", "C670", makeTS(2008, 3, 8)),
      McoMainDiagnosis("Patient_02", "10000123_30000852_2008", "C671", makeTS(2008, 3, 15)),
      McoLinkedDiagnosis("Patient_02", "10000123_20000123_2007", "E05", makeTS(2007, 1, 29)),
      McoLinkedDiagnosis("Patient_02", "10000123_10000543_2006", "E08", makeTS(2005, 12, 24)),
      McoAssociatedDiagnosis("Patient_02", "10000123_20000123_2007", "C66.5", makeTS(2007, 1, 29)),
      McoAssociatedDiagnosis("Patient_02", "10000123_10000543_2006", "C66.9", makeTS(2005, 12, 24)),
      McoMainDiagnosis("Patient_05", "10000123_20000123_2007", "C670", makeTS(2007, 1, 29)),
      McoMainDiagnosis("Patient_06", "10000123_20000123_2007", "C670", makeTS(2007, 1, 29)),
      McoLinkedDiagnosis("Patient_06", "10000123_10000543_2006", "E08", makeTS(2005, 12, 24))
    ).toDS

    val expected = Seq[DiagnosisStat](
      DiagnosisStat("Patient_05", 1, 0, 0),
      DiagnosisStat("Patient_02", 6, 2, 2),
      DiagnosisStat("Patient_06", 1, 0, 1)
    ).toDS()

    val population = DiagnosisCounter.process(input)

    assertDSs(expected, population)

  }

  it should "return zero if dataset is empty" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val input = Seq.empty[Event[Diagnosis]].toDS()

    val population = DiagnosisCounter.process(input)

    assert(population.count() == 0)

  }

}
