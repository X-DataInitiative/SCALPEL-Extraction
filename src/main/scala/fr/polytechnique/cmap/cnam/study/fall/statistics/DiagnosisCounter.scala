package fr.polytechnique.cmap.cnam.study.fall.statistics

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Dataset, Encoder, Encoders, TypedColumn}
import fr.polytechnique.cmap.cnam.etl.events._

case class DiagnosisStat(patientID: String, dp: Int, da: Int, dr: Int) extends Statistical

object DiagnosisCounter extends EventStatistics[Diagnosis, DiagnosisStat] {
  //monoid type in order to aggregate overs diagnosis in dataset
  //https://docs.databricks.com/_static/notebooks/dataset-aggregator.html
  //https://medium.com/build-and-learn/spark-aggregating-your-data-the-fast-way-e37b53314fad
  private val sumOf: TypedColumn[DiagnosisStat, DiagnosisStat] = new Aggregator[DiagnosisStat, DiagnosisStat, DiagnosisStat] with Serializable {
    override def zero: DiagnosisStat = DiagnosisStat("", 0, 0, 0)

    override def reduce(b: DiagnosisStat, a: DiagnosisStat): DiagnosisStat =
      DiagnosisStat("", b.dp + a.dp, b.da + a.da, b.dr + a.dr)

    override def merge(b1: DiagnosisStat, b2: DiagnosisStat): DiagnosisStat =
      DiagnosisStat("", b1.dp + b2.dp, b1.da + b2.da, b1.dr + b2.dr)

    override def finish(reduction: DiagnosisStat): DiagnosisStat = reduction

    override def bufferEncoder: Encoder[DiagnosisStat] = Encoders.product[DiagnosisStat]

    override def outputEncoder: Encoder[DiagnosisStat] = Encoders.product[DiagnosisStat]
  }.toColumn

  override def process(ds: Dataset[Event[Diagnosis]]): Dataset[DiagnosisStat] = {
    val context = ds.sqlContext
    import context.implicits._

    ds.map {
      event =>
        event.category match {
          case McoMainDiagnosis.category => DiagnosisStat(event.patientID, 1, 0, 0)
          case McoAssociatedDiagnosis.category => DiagnosisStat(event.patientID, 0, 1, 0)
          case McoLinkedDiagnosis.category => DiagnosisStat(event.patientID, 0, 0, 1)
          case _ => DiagnosisStat(event.patientID, 0, 0, 0)
        }
    }.groupByKey(_.patientID).agg(sumOf).map {
      case (patientID, DiagnosisStat(_, dp, da, dr)) => DiagnosisStat(patientID, dp, da, dr)
    }

  }
}
