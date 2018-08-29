package fr.polytechnique.cmap.cnam.etl.extractors.mco

import java.sql.Timestamp
import org.apache.spark.sql._
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.EventRowExtractor
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

trait McoEventRowExtractor extends EventRowExtractor with McoSource {

  type Extractor = Row => Option[String]

  case class McoRowExtractor(colName: ColName, codes: Seq[String], builder: EventBuilder) {
    def extract: Extractor = (r: Row) => extractCode(r: Row, colName: ColName, codes: Seq[String])
  }

  def extractors: List[McoRowExtractor]

  def extractorCols: List[String]

  val inputCols: Seq[String] = Seq(
    ColNames.PatientID,
    ColNames.EtaNum,
    ColNames.RsaNum,
    ColNames.Year,
    NewColumns.EstimatedStayStart
  ) ++ extractorCols

  override def extractPatientId(r: Row): String = {
    r.getAs[String](ColNames.PatientID)
  }

  override def extractGroupId(r: Row): String = {
    r.getAs[String](ColNames.EtaNum) + "_" +
      r.getAs[String](ColNames.RsaNum) + "_" +
      r.getAs[Int](ColNames.Year).toString
  }

  override def extractStart(r: Row): Timestamp = r.getAs[Timestamp](NewColumns.EstimatedStayStart)

  def extractCode(r: Row, colName: ColName, codes: Seq[String]): Option[String] = {
    val idx = r.fieldIndex(colName)
    codes.find(!r.isNullAt(idx) && r.getString(idx).startsWith(_))
  }

  def eventFromRow[A <: AnyEvent](
    r: Row, builder: EventBuilder, colName: ColName, codes: Seq[String]): Option[Event[A]] = {

    val foundCode: Option[String] = extractCode(r, colName, codes)

    foundCode.map(
      code => {
        val patientId = extractPatientId(r)
        val groupId = extractGroupId(r)
        val eventDate = extractStart(r)
        builder[A](patientId, groupId, code, extractWeight(r), eventDate, extractEnd(r))
      }
    )
  }

  protected def extract[A <: AnyEvent : ClassTag : TypeTag](mco: DataFrame): Dataset[Event[A]] = {
    import mco.sqlContext.implicits._
    mco.estimateStayStartTime
      .select(inputCols.map(functions.col): _*)
      .flatMap { r =>
        lazy val patientId = extractPatientId(r)
        lazy val groupId = extractGroupId(r)
        lazy val eventDate = extractStart(r)
        lazy val endDate = extractEnd(r)
        lazy val weight = extractWeight(r)

        extractors.flatMap(
          extractor => extractor.extract(r).map(
            code =>
              extractor.builder[A](patientId, groupId, code, weight, eventDate, endDate)
          )
        )
      }.distinct
  }

}
