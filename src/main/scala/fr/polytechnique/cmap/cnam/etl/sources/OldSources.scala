package fr.polytechnique.cmap.cnam.etl.sources

import fr.polytechnique.cmap.cnam.study.StudyConfig.InputPaths
import org.apache.spark.sql.{DataFrame, SQLContext}

class OldSources(
    val dcir: Option[DataFrame] = None,
    val pmsiMco: Option[DataFrame] = None,
    val pmsiMcoCE: Option[DataFrame] = None,
    val pmsiHad: Option[DataFrame] = None,
    val pmsiSsr: Option[DataFrame] = None,
    val irBen: Option[DataFrame] = None,
    val irImb: Option[DataFrame] = None,
    val irPha: Option[DataFrame] = None,
    val dosages: Option[DataFrame] = None)

object OldSources {

  def read(
      sqlContext: SQLContext,
      dcirPath: Option[String] = None,
      pmsiMcoPath: Option[String] = None,
      pmsiMcoCEPath: Option[String] = None,
      pmsiHadPath: Option[String] = None,
      pmsiSsrPath: Option[String] = None,
      irBenPath: Option[String] = None,
      irImbPath: Option[String] = None,
      irPhaPath: Option[String] = None,
      dosagesPath: Option[String] = None): OldSources = {

    new OldSources(
      dcir = dcirPath.map(DcirSource.readAndSanitize(sqlContext, _)),
      pmsiMco = pmsiMcoPath.map(McoSource.readAndSanitize(sqlContext, _)),
      pmsiMcoCE = pmsiMcoCEPath.map(McoSource.readAndSanitize(sqlContext, _)),
      irBen = irBenPath.map(IrBenSource.read(sqlContext, _)),
      irImb = irImbPath.map(IrImbSource.readAndSanitize(sqlContext, _)),
      irPha = irPhaPath.map(IrPhaSource.read(sqlContext, _)),
      dosages = dosagesPath.map(DosagesSource.readAndSanitize(sqlContext, _))
    )
  }

  // for backwards compatibility
  def read(sqlContext: SQLContext, paths: InputPaths): OldSources = {
    this.read(
      sqlContext,
      dcirPath = Option(paths.dcir),
      pmsiMcoPath = Option(paths.pmsiMco),
      //pmsiHadPath = Option(paths.pmsiHad),
      //pmsiSsrPath = Option(paths.pmsiSsr),
      irBenPath = Option(paths.irBen),
      irImbPath = Option(paths.irImb),
      irPhaPath = Option(paths.irPha),
      dosagesPath = Option(paths.dosages)
    )
  }
}
