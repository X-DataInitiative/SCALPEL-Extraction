package fr.polytechnique.cmap.cnam.etl.extractors.ngapacts

import org.apache.spark.sql.{DataFrame, Dataset, Row}

import fr.polytechnique.cmap.cnam.etl.events.{Event, NgapAct}
import fr.polytechnique.cmap.cnam.etl.extractors.dcir.DcirEventRowExtractor

case class DcirNgapActsExtractor(
    ngapCoefficients: Seq[String]) extends DcirEventRowExtractor {

  override def extractors: List[DcirRowExtractor] = List(
    DcirRowExtractor(ColNames.NgapCoefficient, ngapCoefficients, NgapAct))

  override def extractorCols: List[String] = List(ColNames.NgapCoefficient)

  override def extractGroupId(r: Row): String = {
    r.getAs[String](ColNames.ExecPSNum)
  }

  override def extractCode(r: Row, colName: ColName, codes: Seq[String]): Option[String] = {
    val idx = r.fieldIndex(colName)
    codes.find(!r.isNullAt(idx) && r.getDouble(idx).toString == _)
  }

  def extract(dcir: DataFrame): Dataset[Event[NgapAct]] =
    super.extract[NgapAct](dcir)
}
