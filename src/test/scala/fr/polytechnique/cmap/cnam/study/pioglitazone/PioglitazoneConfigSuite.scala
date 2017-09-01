package fr.polytechnique.cmap.cnam.study.pioglitazone

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.study.pioglitazone.PioglitazoneConfig.{PioglitazoneParams, conf}

class PioglitazoneConfigSuite extends SharedContext{

  "PioglitazoneParameters" should "read the right parameters from the config file" in {

    val PioParam = PioglitazoneConfig.pioglitazoneParameters
    val expected = PioglitazoneParams(
      min_purchases = 1 ,
      start_delay = 0,
      purchases_window = 0,
      only_first = false,
      filter_never_sick_patients = false,
      filter_lost_patients = false,
      filter_diagnosed_patients = true,
      diagnosed_patients_threshold = 6,
      filter_delayed_entries = true,
      delayed_entry_threshold = 12
    )
    assert(PioParam == expected)
  }

}
