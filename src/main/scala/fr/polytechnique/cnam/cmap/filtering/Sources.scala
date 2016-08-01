package fr.polytechnique.cnam.cmap.filtering

import org.apache.spark.sql.DataFrame

/**
  * @author Daniel de Paula
  */
class Sources(
    val dcir: Option[DataFrame],
    val pmsiMco: Option[DataFrame],
    val pmsiHad: Option[DataFrame],
    val pmsiSsr: Option[DataFrame],
    val irBen: Option[DataFrame],
    val irImb: Option[DataFrame],
    val irPha: Option[DataFrame]) {

  dcir.foreach(_.cache())
  pmsiMco.foreach(_.cache())
  pmsiHad.foreach(_.cache())
  pmsiSsr.foreach(_.cache())
  irBen.foreach(_.cache())
  irImb.foreach(_.cache())
  irPha.foreach(_.cache())
}
