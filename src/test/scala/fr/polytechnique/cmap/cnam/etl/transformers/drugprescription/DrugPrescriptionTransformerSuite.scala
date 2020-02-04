// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.drugprescription

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{Drug, DrugPrescription, Event}
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class DrugPrescriptionTransformerSuite extends SharedContext {

  "transform" should "combine Drugs that has the same groupID to form a DrugPrescription" in {
    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    //Given

    val input: Dataset[Event[Drug]] = Seq(
      Drug("patient", "CITALOPRAM", 2,"MjAxNC0wOS0wMV8yMDE0LTA3LTE3XzFfMTdfMF8wMUM2NzMxMDBfMTc0OQ==", makeTS(2014, 1, 8)),
      Drug("patient", "ZOLPIDEM", 2,"MjAxNC0wOS0wMV8yMDE0LTA3LTE3XzFfMTdfMF8wMUM2NzMxMDBfMTc0OQ==", makeTS(2014, 1, 8)),
      Drug("patient", "CITALOPRAM", 2,"MjAxNC0wOS0wMV8yMDE0LTA3LTE3XzFfMTdfMF8wMUM2NzMxMDBfMTc0OQ==", makeTS(2014, 3, 12)),
      Drug("patient", "ZOLPIDEM", 2,"MjAxNC0wOS0wMV8yMDE0LTA3LTE3XzFfMTdfMF8wMUM2NzMxMDBfMTc0OQ==", makeTS(2014, 3, 12)),
      Drug("patient", "TIAPRIDE", 2,"MjAxNC0wOS0wMV8yMDE0LTA3LTE3XzFfMTdfMF8wMUM2NzMxMDBfMTc0OQ==", makeTS(2014, 3, 12))
    ).toDS

    val transformer = new DrugPrescriptionTransformer()

    val expected: Dataset[Event[DrugPrescription]] = Seq[Event[DrugPrescription]](
      DrugPrescription("patient", "CITALOPRAM_ZOLPIDEM", 2,"MjAxNC0wOS0wMV8yMDE0LTA3LTE3XzFfMTdfMF8wMUM2NzMxMDBfMTc0OQ==", makeTS(2014, 1, 8)),
      DrugPrescription("patient", "CITALOPRAM_TIAPRIDE_ZOLPIDEM", 2,"MjAxNC0wOS0wMV8yMDE0LTA3LTE3XzFfMTdfMF8wMUM2NzMxMDBfMTc0OQ==", makeTS(2014, 3, 12))
    ).toDS

    val result = transformer.transform(input)

    assertDSs(expected.as[Event[Drug]], result.as[Event[Drug]], true)

  }

  "fromDrugs" should "combine Drugs to form a DrugPrescription"in {
    //Given
    val input: List[Event[Drug]] = List(
      Drug("patient", "CITALOPRAM", 2,"MjAxNC0wOS0wMV8yMDE0LTA3LTE3XzFfMTdfMF8wMUM2NzMxMDBfMTc0OQ==", makeTS(2014, 1, 8)),
      Drug("patient", "ZOLPIDEM", 2,"MjAxNC0wOS0wMV8yMDE0LTA3LTE3XzFfMTdfMF8wMUM2NzMxMDBfMTc0OQ==", makeTS(2014, 1, 8))
    )

    val transformer = new DrugPrescriptionTransformer()

    val expected: Event[DrugPrescription] =
      DrugPrescription("patient", "CITALOPRAM_ZOLPIDEM", 2,"MjAxNC0wOS0wMV8yMDE0LTA3LTE3XzFfMTdfMF8wMUM2NzMxMDBfMTc0OQ==", makeTS(2014, 1, 8))


    val result = transformer.fromDrugs(input)

    assertResult(expected)(result)

  }

}
