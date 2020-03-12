package fr.polytechnique.cmap.cnam.study.dreesChronic.codes

import fr.polytechnique.cmap.cnam.etl.extractors.ngapacts.NgapActClassConfig


 object Amk extends NgapActClassConfig {
   val ngapKeyLetters: Seq[String] = Seq("AMK") // PRS_NAT_REF 3122
   val ngapCoefficients: Seq[String] = Seq(
     "20.0",
     "28.0"
   )
}
