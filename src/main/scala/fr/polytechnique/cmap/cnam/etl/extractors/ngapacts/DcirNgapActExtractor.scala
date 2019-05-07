package fr.polytechnique.cmap.cnam.etl.extractors.ngapacts

import java.sql.Timestamp

import scala.reflect.runtime.universe._
import scala.util.Try
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import fr.polytechnique.cmap.cnam.etl.events.{Event, EventBuilder, NgapAct}
import fr.polytechnique.cmap.cnam.etl.extractors.dcir.DcirExtractor
import fr.polytechnique.cmap.cnam.etl.sources.Sources

class DcirNgapActExtractor(ngapActsConfig: NgapActConfig) extends DcirExtractor[NgapAct] {

  override val columnName: String = ColNames.NgapCoefficient
  val columnNaturePrestation: String = ColNames.NaturePrestation
  override val eventBuilder: EventBuilder = NgapAct


  override def getInput(sources: Sources): DataFrame = sources.dcir.get.select(
    ColNames.PatientID, ColNames.NaturePrestation, ColNames.NgapCoefficient,
    ColNames.Date, ColNames.ExecPSNum
  )

  override def isInExtractorScope(row: Row): Boolean = {
    !row.isNullAt(row.fieldIndex(columnName)) &&
      !row.isNullAt(row.fieldIndex(columnNaturePrestation))
  }

  override def isInStudy(codes: Set[String])(row: Row): Boolean = {
    ngapActsConfig.isInCategory(
      ngapActsConfig.acts_categories,
      row
    )
  }

  override def code: Row => String = (row: Row) => row.getAs[Double](columnName).toString

  override def extractStart(r: Row): Timestamp = {
    Try(super.extractStart(r)) recover {
      case _: NullPointerException => extractFluxDate(r)
    }
  }.get

  override def extractGroupId(r: Row): String = {
    r.getAs[String](ColNames.ExecPSNum)
  }

  override def extract(
    sources: Sources,
    codes: Set[String])
    (implicit ctag: TypeTag[NgapAct]): Dataset[Event[NgapAct]] = {

    val input: DataFrame = getInput(sources)

    import input.sqlContext.implicits._

    {
      if (ngapActsConfig.acts_categories.isEmpty) {
        input.filter(isInExtractorScope _)
      }
      else {
        input.filter(isInExtractorScope _).filter(isInStudy(codes) _)
      }
    }.flatMap(builder _).distinct()
  }
}