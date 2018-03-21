package fr.polytechnique.cmap.cnam.etl.sources

import org.apache.spark.sql.{DataFrame, SQLContext}
import fr.polytechnique.cmap.cnam.etl.sources.data.{DcirSource, McoCeSource, McoSource}
import fr.polytechnique.cmap.cnam.etl.sources.value.{DosagesSource, IrBenSource, IrImbSource, IrPhaSource}
import fr.polytechnique.cmap.cnam.etl.config.StudyConfig.InputPaths

class Sources(
  val dcir: Option[DataFrame] = None,
  val mco: Option[DataFrame] = None,
  val mcoCe: Option[DataFrame] = None,
  val irBen: Option[DataFrame] = None,
  val irImb: Option[DataFrame] = None,
  val irPha: Option[DataFrame] = None,
  val dosages: Option[DataFrame] = None)

object Sources {

  def read(
    sqlContext: SQLContext,
    dcirPath: Option[String] = None,
    mcoPath: Option[String] = None,
    mcoCePath: Option[String] = None,
    irBenPath: Option[String] = None,
    irImbPath: Option[String] = None,
    irPhaPath: Option[String] = None,
    dosagesPath: Option[String] = None): Sources = {

    new Sources(
      dcir = dcirPath.map(DcirSource.read(sqlContext, _)),
      mco = mcoPath.map(McoSource.read(sqlContext, _)),
      mcoCe = mcoCePath.map(McoCeSource.read(sqlContext, _)),
      irBen = irBenPath.map(IrBenSource.read(sqlContext, _)),
      irImb = irImbPath.map(IrImbSource.read(sqlContext, _)),
      irPha = irPhaPath.map(IrPhaSource.read(sqlContext, _)),
      dosages = dosagesPath.map(DosagesSource.read(sqlContext, _))
    )
  }

  def sanitize(sources: Sources): Sources = {
    new Sources(
      dcir = sources.dcir.map(DcirSource.sanitize),
      mco = sources.mco.map(McoSource.sanitize),
      mcoCe = sources.mcoCe.map(McoCeSource.sanitize),
      irBen = sources.irBen.map(IrBenSource.sanitize),
      irImb = sources.irImb.map(IrImbSource.sanitize),
      irPha = sources.irPha.map(IrPhaSource.sanitize),
      dosages = sources.dosages.map(DosagesSource.sanitize)
    )
  }

  def read(sqlContext: SQLContext, paths: InputPaths): Sources = {
    this.read(
      sqlContext,
      dcirPath = Option(paths.dcir),
      mcoPath = Option(paths.pmsiMco),
      irBenPath = Option(paths.irBen),
      irImbPath = Option(paths.irImb),
      irPhaPath = Option(paths.irPha),
      dosagesPath = Option(paths.dosages)
    )
  }
}
