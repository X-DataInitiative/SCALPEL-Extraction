package fr.polytechnique.cmap.cnam.utilities

import scala.reflect.runtime.universe.TypeTag
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

/**
  * Created by burq on 25/06/16.
  */
object functions {

  def classToSchema[CaseClass : TypeTag](): StructType = {
    ScalaReflection.schemaFor[CaseClass].dataType.asInstanceOf[StructType]
  }

}
