package fr.polytechnique.cmap.cnam.etl.sources

import java.sql.Timestamp
import org.apache.spark.sql.{DataFrame, SQLContext}
import fr.polytechnique.cmap.cnam.etl.config.study.StudyConfig.InputPaths
import fr.polytechnique.cmap.cnam.etl.sources.data.{DcirSource, McoCeSource, McoSource, SsrSource}
import fr.polytechnique.cmap.cnam.etl.sources.value.{DosagesSource, IrBenSource, IrImbSource, IrPhaSource}

case class Sources(
  dcir: Option[DataFrame] = None,
  mco: Option[DataFrame] = None,
  mcoCe: Option[DataFrame] = None,
  ssr: Option[DataFrame] = None,
  irBen: Option[DataFrame] = None,
  irImb: Option[DataFrame] = None,
  irPha: Option[DataFrame] = None,
  dosages: Option[DataFrame] = None)

object Sources {

  def sanitize(sources: Sources): Sources = {
    sources.copy(
      dcir = sources.dcir.map(DcirSource.sanitize),
      mco = sources.mco.map(McoSource.sanitize),
      mcoCe = sources.mcoCe.map(McoCeSource.sanitize),
      ssr = sources.ssr.map(SsrSource.sanitize),
      irBen = sources.irBen.map(IrBenSource.sanitize),
      irImb = sources.irImb.map(IrImbSource.sanitize),
      irPha = sources.irPha.map(IrPhaSource.sanitize),
      dosages = sources.dosages.map(DosagesSource.sanitize)
    )
  }

  def sanitizeDates(sources: Sources, studyStart: Timestamp, studyEnd: Timestamp): Sources = {
    sources.copy(
      dcir = sources.dcir.map(DcirSource.sanitizeDates(_, studyStart, studyEnd)),
      mco = sources.mco.map(McoSource.sanitizeDates(_, studyStart, studyEnd)),
      ssr = sources.ssr.map(SsrSource.sanitizeDates(_, studyStart, studyEnd)),
      mcoCe = sources.mcoCe.map(McoCeSource.sanitizeDates(_, studyStart, studyEnd)),
      irBen = sources.irBen,
      irImb = sources.irImb,
      irPha = sources.irPha,
      dosages = sources.dosages
    )
  }

  def read(sqlContext: SQLContext, paths: InputPaths): Sources = {
    this.read(
      sqlContext,
      dcirPath = paths.dcir,
      mcoPath = paths.mco,
      mcoCePath = paths.mcoCe,
      hadPath = paths.had,
      ssrPaths = paths.ssr,
      irBenPath = paths.irBen,
      irImbPath = paths.irImb,
      irPhaPath = paths.irPha,
      dosagesPath = paths.dosages
    )
  }

  def read(
    sqlContext: SQLContext,
    dcirPath: Option[String] = None,
    mcoPath: Option[String] = None,
    mcoCePath: Option[String] = None,
    hadPath: Option[String] = None,
    ssrPaths: Option[List[String]] = None,
    irBenPath: Option[String] = None,
    irImbPath: Option[String] = None,
    irPhaPath: Option[String] = None,
    dosagesPath: Option[String] = None): Sources = {

    Sources(
      dcir = dcirPath.map(DcirSource.read(sqlContext, _)),
      mco = mcoPath.map(McoSource.read(sqlContext, _)),
      mcoCe = mcoCePath.map(McoCeSource.read(sqlContext, _)),
      ssr = ssrPaths.map(SsrSource.readAnnotateJoin(sqlContext, _, "SSR_C")),
      irBen = irBenPath.map(IrBenSource.read(sqlContext, _)),
      irImb = irImbPath.map(IrImbSource.read(sqlContext, _)),
      irPha = irPhaPath.map(IrPhaSource.read(sqlContext, _)),
      dosages = dosagesPath.map(DosagesSource.read(sqlContext, _))
    )
  }
}
