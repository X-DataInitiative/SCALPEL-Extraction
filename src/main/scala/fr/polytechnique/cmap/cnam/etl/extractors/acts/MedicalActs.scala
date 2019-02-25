package fr.polytechnique.cmap.cnam.etl.extractors.acts

import java.sql.Timestamp
import scala.util.Try
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import fr.polytechnique.cmap.cnam._
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.mco._
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.datetime.implicits._
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets

@deprecated("I said so")
class MedicalActs(config: MedicalActsConfig) {

  def extract(sources: Sources): Dataset[Event[MedicalAct]] = {
    val dcirActs = DcirMedicalActs.extract(sources.dcir.get, config.dcirCodes)

    val mcoActs = McoMedicalActs(
      config.mcoCIMCodes,
      config.mcoCCAMCodes
    ).extract(
      sources.mco.get
    )

    lazy val mcoCEActs = McoCEMedicalActs.extract(
      sources.mcoCe.get,
      config.mcoCECodes
    )

    if (sources.mcoCe.isEmpty) {
      unionDatasets(dcirActs, mcoActs)
    }
    else {
      unionDatasets(dcirActs, mcoActs, mcoCEActs)
    }
  }
}
