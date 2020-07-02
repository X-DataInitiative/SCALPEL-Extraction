package fr.polytechnique.cmap.cnam.study.dreesChronic.codes

import fr.polytechnique.cmap.cnam.etl.extractors.events.ngapacts.NgapActClassConfig


 object Amk extends NgapActClassConfig(
   ngapKeyLetters = Seq("AMK"), // PRS_NAT_REF 3122
    ngapCoefficients = Seq(
     "20.0",
     "28.0"
   )
)
