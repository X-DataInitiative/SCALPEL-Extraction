package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.DataFrame

/**
  * Wrapper class for the DataFrames containing the data from the source tables
  *
  * @author Daniel de Paula
  */
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
}