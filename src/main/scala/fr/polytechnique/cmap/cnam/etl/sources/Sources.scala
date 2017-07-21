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
    val dosages: Option[DataFrame] = None) {

}

object Sources {

  def apply(
    sqlContext: SQLContext,
    dcirPath: String = "",
    pmsiMcoPath: String = "",
    pmsiHadPath: String = "",
    pmsiSsrPath: String = "",
    irBenPath: String = "",
    irImbPath: String = "",
    irPhaPath: String = "",
    dosagesPath: String = ""): Sources = {

    new Sources(
      dcir = if (dcirPath.isEmpty) None else Some(Dcir.read(sqlContext, dcirPath)),
      pmsiMco = if (pmsiMcoPath.isEmpty) None else Some(Mco.read(sqlContext, pmsiMcoPath)),
      irBen = if (irBenPath.isEmpty) None else Some(IrBen.read(sqlContext, irBenPath)),
      irImb = if (irImbPath.isEmpty) None else Some(IrImb.read(sqlContext, irImbPath)),
      irPha = if (irPhaPath.isEmpty) None else Some(IrPha.read(sqlContext, irPhaPath)),
      dosages = if (dosagesPath.isEmpty) None else Some(Dosages.read(sqlContext, dosagesPath))
    )
  }

  def read(sqlContext: SQLContext, paths: InputPaths): Sources = {
    new Sources(
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