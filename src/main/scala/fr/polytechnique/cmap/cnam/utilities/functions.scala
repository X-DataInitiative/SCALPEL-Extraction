package fr.polytechnique.cmap.cnam.utilities

import java.sql.Timestamp
import scala.reflect.runtime.universe.TypeTag
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

object functions {

  implicit class IntFunctions(num: Int) {
    def bw(lower: Int, upper: Int): Boolean = num >= lower && num <= upper
  }

  def classToSchema[CaseClass : TypeTag](): StructType = {
    ScalaReflection.schemaFor[CaseClass].dataType.asInstanceOf[StructType]
  }

  def makeTS(year: Int, month: Int, day: Int, hour: Int = 0, minute: Int = 0, second: Int = 0): Timestamp = {
    if(!year.bw(1000, 3000) || !month.bw(1, 12) || !day.bw(1,31) || !hour.bw(0, 23) ||
        !minute.bw(0, 59) || !second.bw(0,59))
      throw new java.lang.IllegalArgumentException("Argument out of bounds")

    Timestamp.valueOf(f"$year%04d-$month%02d-$day%02d $hour%02d:$minute%02d:$second%02d")
  }
}
