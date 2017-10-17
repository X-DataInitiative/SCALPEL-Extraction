package fr.polytechnique.cmap.cnam.study.rosiglitazone

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.study.rosiglitazone.RosiglitazoneConfig._

class RosiglitazoneConfigSuite extends SharedContext {

  "RosiglitazoneParameters" should "read the right parameters from the config file" in {

    /*
  The parameters read from conf file :
      min_purchases = 1 // 1 or 2
      start_delay = 0 // can vary from 0 to 3
      purchases_window = 0 // always 0
      only_first = false // can be always false and handled in python / C++, but not soon
      filter_never_sick_patients = false // always true
      filter_lost_patients = false //keep it
      filter_diagnosed_patients = true // keep it
      diagnosed_patients_threshold = 6 // keep it, maybe remove the previous one and set false when this param is 0
      filter_delayed_entries = true // keep it
      delayed_entry_threshold = 12 // keep it, maybe remove the previous one and set false when this param is 0
   */

    val rosiParam = RosiglitazoneConfig.rosiglitazoneParameters
    val expected = RosiglitazoneParams(
      DrugsParams(min_purchases = 1, start_delay = 0, purchases_window = 0, only_first = false),
      StudyParams(delayed_entry_threshold = 12),
      FiltersParams(
        filter_never_sick_patients = false,
        filter_lost_patients = false,
        filter_diagnosed_patients = true,
        diagnosed_patients_threshold = 6,
        filter_delayed_entries = true
      )
    )
    assert(rosiParam == expected)
  }
}
