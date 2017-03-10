package fr.polytechnique.cmap.cnam.etl.sources

import org.apache.spark.sql.{DataFrame, SQLContext}
import fr.polytechnique.cmap.cnam.etl.old_root.FilteringConfig.InputPaths

class Sources(
    val dcir: Option[DataFrame] = None,
    val pmsiMco: Option[DataFrame] = None,
    val pmsiHad: Option[DataFrame] = None,
    val pmsiSsr: Option[DataFrame] = None,
    val irBen: Option[DataFrame] = None,
    val irImb: Option[DataFrame] = None,
    val irPha: Option[DataFrame] = None,
    val dosages: Option[DataFrame] = None)

object Sources {

  def apply(
      dcir: Option[DataFrame] = None,
      pmsiMco: Option[DataFrame] = None,
      pmsiHad: Option[DataFrame] = None,
      pmsiSsr: Option[DataFrame] = None,
      irBen: Option[DataFrame] = None,
      irImb: Option[DataFrame] = None,
      irPha: Option[DataFrame] = None,
      dosages: Option[DataFrame] = None) = {
    new Sources(dcir, pmsiMco, pmsiHad, pmsiSsr, irBen, irImb, irPha, dosages)
  }

  def read(sqlContext: SQLContext, paths: InputPaths): Sources = {
    Sources(
      // todo: think about upperBoundIrphaQuantity parameter
      dcir = Some(Dcir.read(sqlContext, paths.dcir)),
      pmsiMco = Some(Mco.read(sqlContext, paths.pmsiMco)),
      // pmsiHad = Some(Had(paths.pmsiHad)),
      // pmsiSsr = Some(Ssr(paths.pmsiSsr)),
      irBen = Some(IrBen.read(sqlContext, paths.irBen)),
      irImb = Some(IrImb.read(sqlContext, paths.irImb)),
      irPha = Some(IrPha.read(sqlContext, paths.irPha)),
      dosages = Some(Dosages.read(sqlContext, paths.dosages))
    )
  }
}