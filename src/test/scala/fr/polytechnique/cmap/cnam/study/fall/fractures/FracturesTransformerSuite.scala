package fr.polytechnique.cmap.cnam.study.fall.fractures

import me.danielpes.spark.datetime.implicits._
import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.study.fall.FallMain.CCAMExceptions
import fr.polytechnique.cmap.cnam.study.fall.config.FallConfig
import fr.polytechnique.cmap.cnam.study.fall.config.FallConfig.FracturesConfig
import fr.polytechnique.cmap.cnam.util.functions.makeTS


class FracturesTransformerSuite extends SharedContext {

  "transform" should "build fractures from acts and diagnoses" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    //Given
    val defaultConf = FallConfig.load("", "test")
    val testConf = defaultConf.copy(outcomes = FracturesConfig(fallFrame = 3.months))
    val acts: Dataset[Event[MedicalAct]] = Seq(
      //pubic ambulatory acts
      McoCEAct("georgette", DcirAct.groupID.PublicAmbulatory, "MZMP007", makeTS(2010, 2, 6)),
      McoCEAct("george", DcirAct.groupID.PublicAmbulatory, "whatever", makeTS(2010, 2, 6)),
      DcirAct("john", DcirAct.groupID.PublicAmbulatory, "MZMP007", makeTS(2010, 2, 6)),
      //private ambulatory acts
      DcirAct("riri", DcirAct.groupID.PrivateAmbulatory, "NBEP002", makeTS(2007, 1, 1)),
      DcirAct("fifi", DcirAct.groupID.PrivateAmbulatory, "stupidcode", makeTS(2007, 1, 1)),
      DcirAct("loulou", DcirAct.groupID.PublicAmbulatory, "stupidcode", makeTS(2007, 1, 1)),
      //hospitalization acts
      McoCCAMAct("Paul", "1", "LJGA001", makeTS(2017, 7, 20)),
      //liberal acts
      DcirAct("Pierre", DcirAct.groupID.Liberal, "MADP001", makeTS(2017, 7, 18)),
      DcirAct("Ben", DcirAct.groupID.Liberal, "MZMP007", makeTS(2017, 7, 18)),
      DcirAct("Sam", DcirAct.groupID.Liberal, "HBED009", makeTS(2015, 7, 18)),
      DcirAct("Sam", DcirAct.groupID.Liberal, "HBED009", makeTS(2015, 9, 18))
    ).toDS()
    val liberalActs = acts.filter(act => act.groupID == DcirAct.groupID.Liberal && !CCAMExceptions.contains(act.value))
    val diagnoses = Seq(
      //hospitalization diagnoses
      MainDiagnosis("emile", "3", "S222", makeTS(2017, 7, 18)),
      MainDiagnosis("Paul", "1", "S42.54678", makeTS(2017, 7, 20)),
      MainDiagnosis("Paul", "7", "hemorroides", makeTS(2017, 1, 2)),
      AssociatedDiagnosis("Jacques", "8", "qu'est-ce-que tu fais là?", makeTS(2017, 7, 18))
    ).toDS

    val expectedOutcomes = Seq(
      //hospitalization
      Outcome("emile", "Ribs", "hospitalized_fall", makeTS(2017, 7, 18)),
      //private ambulatory
      Outcome("riri", "FemurExclusionCol", PrivateAmbulatoryFractures.outcomeName, makeTS(2007, 1, 1)),
      //public ambulatory
      Outcome("georgette", "MembreSuperieurDistal", PublicAmbulatoryFractures.outcomeName, makeTS(2010, 2, 6)),
      //liberal
      Outcome("Pierre", "Clavicule", "Liberal", makeTS(2017, 7, 18)),
      Outcome("Ben", "MembreSuperieurDistal", "Liberal", makeTS(2017, 7, 18)),
      Outcome("Sam", "CraneFace", "Liberal", makeTS(2015, 7, 18))

    ).toDS

    //When
    val result = new FracturesTransformer(testConf).transform(liberalActs, acts, diagnoses)


    //Then
    assertDSs(result, expectedOutcomes)
  }

}
