package fr.polytechnique.cmap.cnam.utilities

import java.sql.Timestamp
import scala.reflect.runtime.universe.TypeTag
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

object functions {

  def classToSchema[CaseClass : TypeTag](): StructType = {
    ScalaReflection.schemaFor[CaseClass].dataType.asInstanceOf[StructType]
  }

  def makeTS(year: Int, month: Int, day: Int, hour: Int = 0, minute: Int = 0, second: Int = 0): Timestamp = {
    Timestamp.valueOf(f"$year%04d-$month%02d-$day%02d $hour%02d:$minute%02d:$second%02d")
  }
}
