package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.DataFrame

/**
  * Wrapper class for the DataFrames containing the data from the source tables
  *
  * @author Daniel de Paula
  */
class Sources(
    val dcir: Option[DataFrame],
    val pmsiMco: Option[DataFrame],
    val pmsiHad: Option[DataFrame],
    val pmsiSsr: Option[DataFrame],
    val irBen: Option[DataFrame],
    val irImb: Option[DataFrame],
    val irPha: Option[DataFrame])
