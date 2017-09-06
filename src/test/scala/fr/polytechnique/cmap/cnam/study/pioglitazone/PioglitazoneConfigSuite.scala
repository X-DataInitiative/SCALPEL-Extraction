package fr.polytechnique.cmap.cnam.study.pioglitazone

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.study.pioglitazone.PioglitazoneConfig._

class PioglitazoneConfigSuite extends SharedContext{

  "PioglitazoneParameters" should "read the right parameters from the config file" in {

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

    val PioParam = PioglitazoneConfig.pioglitazoneParameters
        val expected = PioglitazoneParams(
          DrugsParams(min_purchases = 1, start_delay = 0, purchases_window = 0, only_first = false),
          MedicalActParams(),
          DiagnosesParams(),
          StudyParams(delayed_entry_threshold = 12),
          FiltersParams(
            filter_never_sick_patients = false,
            filter_lost_patients = false,
            filter_diagnosed_patients = true,
            diagnosed_patients_threshold = 6,
            filter_delayed_entries = true
          ))
    assert(PioParam == expected)
  }
  }



