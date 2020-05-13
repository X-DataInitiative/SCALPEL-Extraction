// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.sources

import java.sql.Timestamp
import org.apache.spark.sql.{DataFrame, SQLContext}
import fr.polytechnique.cmap.cnam.etl.config.study.StudyConfig.InputPaths
import fr.polytechnique.cmap.cnam.etl.sources.data._
import fr.polytechnique.cmap.cnam.etl.sources.value._

case class Sources(
  dcir: Option[DataFrame] = None,
  mco: Option[DataFrame] = None,
  mcoCe: Option[DataFrame] = None,
  ssr: Option[DataFrame] = None,
  ssrCe: Option[DataFrame] = None,
  had: Option[DataFrame] = None,
  irBen: Option[DataFrame] = None,
  irImb: Option[DataFrame] = None,
  irPha: Option[DataFrame] = None,
  irNat: Option[DataFrame] = None,
  dosages: Option[DataFrame] = None)

object Sources {
  /** Sanitize all sources with usual filters for snds analysis.
    *
    * @param sources An instance containing all available SNDS data and value tables.
    * @return
    */
  def sanitize(sources: Sources): Sources = {
    sources.copy(
      dcir = sources.dcir.map(DcirSource.sanitize),
      mco = sources.mco.map(McoSource.sanitize),
      mcoCe = sources.mcoCe.map(McoCeSource.sanitize),
      ssr = sources.ssr.map(SsrSource.sanitize),
      ssrCe = sources.ssrCe.map(SsrCeSource.sanitize),
      had = sources.had.map(HadSource.sanitize),
      irBen = sources.irBen.map(IrBenSource.sanitize),
      irImb = sources.irImb.map(IrImbSource.sanitize),
      irPha = sources.irPha.map(IrPhaSource.sanitize),
      irNat = sources.irNat.map(IrNatSource.sanitize),
      dosages = sources.dosages.map(DosagesSource.sanitize)
    )
  }

  /** Filter sources to keep only data concerning the study period.
    *
    * @param sources An instance containing all available SNDS data and value tables.
    * @param studyStart
    * @param studyEnd
    * @return
    */
  def sanitizeDates(sources: Sources, studyStart: Timestamp, studyEnd: Timestamp): Sources = {
    sources.copy(
      dcir = sources.dcir.map(DcirSource.sanitizeDates(_, studyStart, studyEnd)),
      mco = sources.mco.map(McoSource.sanitizeDates(_, studyStart, studyEnd)),
      ssr = sources.ssr.map(SsrSource.sanitizeDates(_, studyStart, studyEnd)),
      had = sources.had.map(HadSource.sanitizeDates(_, studyStart, studyEnd)),
      mcoCe = sources.mcoCe.map(McoCeSource.sanitizeDates(_, studyStart, studyEnd)),
      ssrCe = sources.ssrCe.map(SsrCeSource.sanitizeDates(_, studyStart, studyEnd)),
      irBen = sources.irBen,
      irImb = sources.irImb,
      irPha = sources.irPha,
      irNat = sources.irNat,
      dosages = sources.dosages
    )
  }

  /** Read all source dataframe.
    *
    * @param sqlContext Spark Context needed to fetch data
    * @param paths
    * @return
    */
  def read(sqlContext: SQLContext, paths: InputPaths,fileFormat: String): Sources = {
    this.read(
      sqlContext,
      dcirPath = paths.dcir,
      mcoPath = paths.mco,
      mcoCePath = paths.mcoCe,
      hadPath = paths.had,
      ssrPaths = paths.ssr,
      ssrCePath = paths.ssrCe,
      irBenPath = paths.irBen,
      irImbPath = paths.irImb,
      irPhaPath = paths.irPha,
      irNatPath = paths.irNat,
      dosagesPath = paths.dosages,
      fileFormat = fileFormat
    )
  }

  def read(
    sqlContext: SQLContext,
    dcirPath: Option[String] = None,
    mcoPath: Option[String] = None,
    mcoCePath: Option[String] = None,
    hadPath: Option[String] = None,
    ssrPaths: Option[String] = None,
    ssrCePath: Option[String] = None,
    irBenPath: Option[String] = None,
    irImbPath: Option[String] = None,
    irPhaPath: Option[String] = None,
    irNatPath: Option[String] = None,
    dosagesPath: Option[String] = None,
    fileFormat: String): Sources = {

    Sources(
      dcir = dcirPath.map(DcirSource.read(sqlContext, _,fileFormat)),
      mco = mcoPath.map(McoSource.read(sqlContext, _,fileFormat)),
      mcoCe = mcoCePath.map(McoCeSource.read(sqlContext, _,fileFormat)),
      ssr = ssrPaths.map(SsrSource.read(sqlContext, _,fileFormat)),
      ssrCe = ssrCePath.map(SsrCeSource.read(sqlContext, _,fileFormat)),
      had = hadPath.map(HadSource.read(sqlContext, _,fileFormat)),
      irBen = irBenPath.map(IrBenSource.read(sqlContext, _,fileFormat)),
      irImb = irImbPath.map(IrImbSource.read(sqlContext, _,fileFormat)),
      irPha = irPhaPath.map(IrPhaSource.read(sqlContext, _,fileFormat)),
      irNat = irNatPath.map(IrNatSource.read(sqlContext, _,fileFormat)),
      dosages = dosagesPath.map(DosagesSource.read(sqlContext, _))
    )
  }
}
