// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.util

/**
  * Utility object for creating HDFS Paths (org.apache.hadoop.fs.Path)
  * It allows the creation of Paths without the "new" keyword ands add some new signatures
  *
  * Note that a type alias is present in the util's package object
  * (type Path = org.apache.hadoop.fs.Path), so we should always import only
  *   fr.polytechnique.cmap.cnam.util.Path instead of the Hadoop one
  */
object Path {

  // We re-tag the path type for local readability
  private type HDFSPath = Path

  def apply(left: HDFSPath, right: HDFSPath): HDFSPath = new HDFSPath(left, right)

  def apply(left: HDFSPath, right: String): HDFSPath = new HDFSPath(left, right)

  def apply(left: String, right: HDFSPath): HDFSPath = new HDFSPath(left, right)

  def apply(left: String, right: String): HDFSPath = new HDFSPath(left, right)

  /**
    * New factory methods for Paths. Allows passing many strings at once.
    *
    * @param parts One or more string parts to form a Path.
    * @return
    */
  def apply(parts: String*): HDFSPath = Path(Path(parts.head), parts.tail: _*)

  // The following 5 signatures are only proxies for the Hadoop Path constructor, to remove the need for the "new" keyword.
  def apply(stringPath: String): HDFSPath = new HDFSPath(stringPath)

  /**
    * New factory methods for Paths. Allows passing many strings at once.
    *
    * @param head The base Path.
    * @param tail One or more strings to be concatenated to the base Path and form a new one
    * @return
    */
  def apply(head: HDFSPath, tail: String*): HDFSPath = {
    tail.foldLeft(head) { (path, tailHead) =>
      new Path(path, tailHead)
    }
  }
}
