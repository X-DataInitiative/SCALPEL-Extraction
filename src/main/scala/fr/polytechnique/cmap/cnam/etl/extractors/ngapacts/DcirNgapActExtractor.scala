package fr.polytechnique.cmap.cnam.etl.extractors.ngapacts

import java.sql.Timestamp

import scala.reflect.runtime.universe._
import scala.util.Try
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import fr.polytechnique.cmap.cnam.etl.events.{DcirNgapAct, Event, EventBuilder, NgapAct}
import fr.polytechnique.cmap.cnam.etl.extractors.dcir.DcirExtractor
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import org.apache.spark.sql.functions.col

class DcirNgapActExtractor(ngapActsConfig: NgapActConfig) extends DcirExtractor[NgapAct] {

  override val columnName: String = ColNames.NaturePrestation
  val ngapKeyLetter: String = "PRS_NAT_CB2"

  override val eventBuilder: EventBuilder = DcirNgapAct


  override def getInput(sources: Sources): DataFrame = {
    val neededColumns: List[Column] = List(
      ColNames.PatientID, ColNames.NaturePrestation, ColNames.NgapCoefficient,
      ColNames.Date, ColNames.ExecPSNum, ColNames.DcirFluxDate, ngapKeyLetter
    ).map(colName => col(colName))


    lazy val irNat = sources.irNat.get
    lazy val dcir = sources.dcir.get

    lazy val df: DataFrame = dcir.join(irNat, dcir("PRS_NAT_REF").cast("String") === irNat("PRS_NAT"))

    df.select(neededColumns: _*)
  }

  override def isInExtractorScope(row: Row): Boolean = {
    !row.isNullAt(row.fieldIndex(ngapKeyLetter))
  }

  override def isInStudy(codes: Set[String])(row: Row): Boolean = {
    ngapActsConfig.dcirIsInCategory(
      ngapActsConfig.acts_categories,
      row
    )
  }

  override def code: Row => String = (row: Row) => {
    row.getAs[Int](ColNames.NaturePrestation).toString + "_" +
      row.getAs[String](ngapKeyLetter) + "_" +
      row.getAs[Double](ColNames.NgapCoefficient).toString
  }

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