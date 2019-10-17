// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.util.reporting

import java.io._
import scala.collection._

/**
  * Represents the reporting metadata for a single operation.
  * An operation can be any method that touches a DataFrame, including but not limited to: readers,
  * extractors, transformers and filters.
  */
case class OperationMetadata(
  name: String,
  inputs: List[String],
  outputType: OperationType,
  outputPath: String = "",
  populationPath: String = "")
  extends JsonSerializable

object OperationMetadata {

  // REASON: https://stackoverflow.com/a/22375260/795574
  def deserialize(file : String) : mutable.Map[String, OperationMetadata] = {
    val ois = new ObjectInputStream(new FileInputStream(file)) {
      override def resolveClass(desc: java.io.ObjectStreamClass): Class[_] = {
        try {
          Class.forName(desc.getName, false, getClass.getClassLoader)
        }
        catch {
          case ex: ClassNotFoundException => super.resolveClass(desc)
        }
      }
    }
    val meta : java.util.Map[String, OperationMetadata]
      = ois.readObject.asInstanceOf[java.util.Map[String, OperationMetadata]]
    ois.close
    JavaConversions.mapAsScalaMap[String, OperationMetadata](meta)
  }

  // Scala JavaConversions has issues with serialization
  def scalaToJavaHashMapConverter(scalaMap: mutable.HashMap[String, OperationMetadata]) = {
    val javaHashMap = new java.util.HashMap[String, OperationMetadata]()
    scalaMap.foreach(entry => javaHashMap.put(entry._1, entry._2))
    javaHashMap
  }

  def serialize(file : String, meta : mutable.HashMap[String, OperationMetadata]) = {
    val oos = new ObjectOutputStream(new FileOutputStream(file))
    oos.writeObject(scalaToJavaHashMapConverter(meta))
    oos.close
  }
}
