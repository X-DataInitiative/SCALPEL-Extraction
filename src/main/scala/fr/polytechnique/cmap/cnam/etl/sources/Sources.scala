package fr.polytechnique.cmap.cnam.etl.sources

import org.apache.spark.sql.{DataFrame, SQLContext}
import fr.polytechnique.cmap.cnam.study.StudyConfig.InputPaths

class Sources(
    val dcir: Option[DataFrame] = None,
    val pmsiMco: Option[DataFrame] = None,
    val pmsiMcoCE: Option[DataFrame] = None,
    val pmsiHad: Option[DataFrame] = None,
    val pmsiSsr: Option[DataFrame] = None,
    val irBen: Option[DataFrame] = None,
    val irImb: Option[DataFrame] = None,
    val irPha: Option[DataFrame] = None,
    val dosages: Option[DataFrame] = None)

object Sources {

  // null defaults to fall automatically to None when converted to Option
  def read(
      sqlContext: SQLContext,
      dcirPath: String = null,
      pmsiMcoPath: String = null,
      pmsiMcoCEPath: String = null,
      pmsiHadPath: String = null,
      pmsiSsrPath: String = null,
      irBenPath: String = null,
      irImbPath: String = null,
      irPhaPath: String = null,
      dosagesPath: String = null): Sources = {

    new Sources(
      dcir = Option(dcirPath).map(Dcir.read(sqlContext, _)),
      pmsiMco = Option(pmsiMcoPath).map(Mco.read(sqlContext, _)),
      pmsiMcoCE = Option(pmsiMcoCEPath).map(Mco.read(sqlContext, _)),
      irBen = Option(irBenPath).map(IrBen.read(sqlContext, _)),
      irImb = Option(irImbPath).map(IrImb.read(sqlContext, _)),
      irPha = Option(irPhaPath).map(IrPha.read(sqlContext, _)),
      dosages = Option(dosagesPath).map(Dosages.read(sqlContext, _))
    )
  }

  // for backwards compatibility
  def read(sqlContext: SQLContext, paths: InputPaths): Sources = {
    this.read(
      sqlContext,
      paths.dcir,
      paths.pmsiMco,
      paths.pmsiHad,
      paths.pmsiSsr,
      paths.irBen,
      paths.irImb,
      paths.irPha,
      paths.dosages
    )
  }
}
