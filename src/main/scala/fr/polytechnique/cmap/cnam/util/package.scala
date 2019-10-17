// License: BSD 3 clause

package fr.polytechnique.cmap.cnam

import java.text.SimpleDateFormat
import java.util.Date

package object util {
  /**
    * Type alias to avoid the need for importing Path from the hadoop java API
    */
  type Path = org.apache.hadoop.fs.Path

  implicit class RichPath(path: Path) {

    def withTimestampSuffix(
      date: Date = new Date(),
      format: String = "_yyyy_MM_dd_HH_mm_ss",
      oldSuffix: String = "/"): Path = {
      Path(
        path.toString
          .stripSuffix(oldSuffix)
          .concat(new SimpleDateFormat(format).format(date))
      )
    }
  }

}