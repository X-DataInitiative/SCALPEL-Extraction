// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.util

import java.util.{Locale, TimeZone}

trait Locales {
  Locale.setDefault(Locale.US)
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
}
