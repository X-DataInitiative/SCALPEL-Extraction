package fr.polytechnique.cmap.cnam.util.reporting

import org.json4s.DefaultFormats
import org.json4s.native.Serialization

trait JsonSerializable {
  def toJsonString(pretify: Boolean = true): String = {
    if(pretify) Serialization.writePretty(this)(DefaultFormats)
    else Serialization.write(this)(DefaultFormats)
  }
}
