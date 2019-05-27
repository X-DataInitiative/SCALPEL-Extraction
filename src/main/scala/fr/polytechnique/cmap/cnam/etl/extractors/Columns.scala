package fr.polytechnique.cmap.cnam.etl.extractors

import java.lang.reflect.Field;
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col


trait Columns extends Serializable {
  def isEmpty(x: String) = x == null || x.trim.isEmpty
  def validate(is : Boolean, f : Field, field_value : String) : String = {
    f.setAccessible(is)
    field_value
  }
  def getColumns : Array[String] = {
    this.getClass.getDeclaredFields.map { f =>
      val is = f.isAccessible
      f.setAccessible(true)
      val g = f.get(this)
      val ret = g match {
        case null => ""
        case r: String => validate(is, f, r)
        case _ => ""
      }
      ret
    }.filter(_.nonEmpty)
  }
}
