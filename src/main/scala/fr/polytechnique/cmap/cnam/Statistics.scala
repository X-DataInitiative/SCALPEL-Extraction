package fr.polytechnique.cmap.cnam

import org.apache.spark.sql.DataFrame

/**
  * Created by sathiya on 28/06/16.
  */
object Statistics {

  implicit class Statistic(src: DataFrame){

    def describe = {

    }

  }

}
