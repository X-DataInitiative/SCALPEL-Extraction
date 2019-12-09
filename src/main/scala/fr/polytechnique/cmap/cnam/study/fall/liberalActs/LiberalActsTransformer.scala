package fr.polytechnique.cmap.cnam.study.fall.liberalActs

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{DcirAct, Event, MedicalAct}
import fr.polytechnique.cmap.cnam.study.fall.codes.FractureCodes

trait LiberalActsTransformer extends FractureCodes {
  val name: String = "liberal_acts"
}

object LiberalActsTransformer extends LiberalActsTransformer {
  def transform(acts: Dataset[Event[MedicalAct]]): Dataset[Event[MedicalAct]] = {
    acts.filter(act => act.groupID == DcirAct.groupID.Liberal && !CCAMExceptions.contains(act.value))
  }
}
