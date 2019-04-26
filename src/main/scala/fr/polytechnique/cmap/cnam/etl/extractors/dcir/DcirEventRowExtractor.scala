package fr.polytechnique.cmap.cnam.etl.extractors.dcir

import java.sql.Timestamp
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import org.apache.spark.sql._
import fr.polytechnique.cmap.cnam.util.datetime.implicits._
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.EventRowExtractor

trait DcirEventRowExtractor extends EventRowExtractor with DcirSource {

  protected type Extractor = Row => Option[String]

  protected case class DcirRowExtractor(colName: ColName, codes: Seq[String], builder: EventBuilder) {
    def extract: Extractor = (r: Row) => extractCode(r: Row, colName: ColName, codes: Seq[String])
  }

  def extractors: List[DcirRowExtractor]

  def extractorCols: List[String]

  val inputCols: Seq[String] = Seq(
    ColNames.PatientID,
    ColNames.ExecPSNum,
    ColNames.DcirEventStart,
    ColNames.DcirFluxDate
  ) ++ extractorCols

  override def extractPatientId(r: Row): String = {
    r.getAs[String](ColNames.PatientID)
  }

  def getStart(r: Row): Timestamp= {
    r.getAs[java.util.Date](ColNames.DcirEventStart).toTimestamp
  }

  def getFluxDate(r: Row): Timestamp= {
    r.getAs[java.util.Date](ColNames.DcirFluxDate).toTimestamp
  }

  override def extractStart(r: Row): Timestamp= {
    /**
      * There are some events in the DCIR (~2.6E-2 %) that don't have a start date (EXE_SOI_DTD).
      * They are mainly indemnities unrelated to a medical event.
      * For these events, we approximate the start by the first date of integration
      * in the SI (FLX_DIS_DTD).
      */
    val idx = r.fieldIndex(ColNames.DcirEventStart)
    val isDate = r.isNullAt(idx)
    isDate match {
      case true => getFluxDate(r)
      case false => getStart(r)
    }
  }

  def extractCode(r: Row, colName: ColName, codes: Seq[String]): Option[String] = {
    val idx = r.fieldIndex(colName)
    codes.find(!r.isNullAt(idx) && r.getInt(idx).toString == _)
  }

  protected def extract[A <: AnyEvent : ClassTag : TypeTag](dcir: DataFrame): Dataset[Event[A]] = {
    import dcir.sqlContext.implicits._
    dcir.select(inputCols.map(functions.col): _*)
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
