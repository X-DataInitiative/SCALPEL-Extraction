// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.interaction

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions
import fr.polytechnique.cmap.cnam.etl.datatypes.{LeftRemainingPeriod, Period, RemainingPeriod, RightRemainingPeriod}
import fr.polytechnique.cmap.cnam.etl.events.{Event, Exposure, Interaction}
import fr.polytechnique.cmap.cnam.util.functions._

case class NLevelInteractionTransformer(config: InteractionTransformerConfig) extends InteractionTransformer {
  self =>

  def joinTwoExposureNDataSet(right: Dataset[ExposureN], left: Dataset[ExposureN]): Dataset[ExposureN] = {
    val sqlCtx = right.sqlContext
    import sqlCtx.implicits._
    right
      .joinWith(
        left,
        left(Event.Columns.PatientID) === right(Event.Columns.PatientID) && !left("values").geq(right("values"))
      )
      .flatMap(e => e._1.intersect(e._2))
      .repartition(functions.col("patientID"), functions.col("values"))
      .cache()
  }

  private def wrapper(right: Dataset[ExposureN], left: Option[Dataset[ExposureN]]): Option[Dataset[ExposureN]] =
    left.flatMap(l => Some(self.joinTwoExposureNDataSet(right, l)))

  def elevateToExposureN(exposures: Dataset[Event[Exposure]], n: Int): List[Dataset[ExposureN]] = {
    val sqlCtx = exposures.sqlContext
    import sqlCtx.implicits._

    val zero = Option(
      sqlCtx
        .createDataset(exposures.map(e => ExposureN(e.patientID, Set(e.value), Period(e.start, e.end.get))).rdd)
        .repartition(functions.col("patientID"), functions.col("values")).cache()
    )

    List
      .fill(n - 1)(
        sqlCtx
          .createDataset(exposures.map(e => ExposureN(e.patientID, Set(e.value), Period(e.start, e.end.get))).rdd)
          .repartition(functions.col("patientID"), functions.col("values")).cache()
      )
      .scanRight(zero)(wrapper).flatten
  }

  def trickleDownExposureN(ens: List[Dataset[ExposureN]]): List[Dataset[ExposureN]] = {
    val sqlCtx = ens.head.sqlContext
    import sqlCtx.implicits._

    ens.map(dataset => dataset.flatMap(e => e.toLowerLevelInvolvedExposureN))
  }

  def reduceHigherExposuresNFromLowerExposures(higher: List[Dataset[ExposureN]], lower: List[Dataset[ExposureN]])
  : List[Dataset[LeftRemainingPeriod[ExposureN]]] = {
    val sqlCtx = higher.head.sqlContext
    import sqlCtx.implicits._
    lower.zip(higher).map(
      e => {
        val down1 = e._1
        val high = e._2
        high.joinWith(
          down1,
          high("patientID") === down1("patientID") && high("values") === down1("values"),
          "left"
        ).groupByKey(e => e._1)
          .flatMapGroups(
            (e, i) => {
              val sortedPeriods = i.map(_._2).toList.sortBy(_.period.start)
              RemainingPeriod.delimitPeriods(
                RightRemainingPeriod(e),
                sortedPeriods.map(e => LeftRemainingPeriod[ExposureN](e)),
                List.empty[LeftRemainingPeriod[ExposureN]]
              )
            }

          )
      }
    )
  }

  override def transform(exposures: Dataset[Event[Exposure]]): Dataset[Event[Interaction]] = {
    val exposureN = self.elevateToExposureN(exposures, config.level)

    val sqlCtx = exposures.sqlContext
    import sqlCtx.implicits._
    self
      .reduceHigherExposuresNFromLowerExposures(exposureN.reverse, self.trickleDownExposureN(exposureN.reverse.drop(1)))
      .foldLeft(exposureN.head.map(l => l.toInteraction))((acc, b) => acc.union(b.map(l => l.e.toInteraction)))
      .distinct()
  }
}