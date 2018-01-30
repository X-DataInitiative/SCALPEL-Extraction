package fr.polytechnique.cmap.cnam

package object util {
  /**
    * Type alias to avoid the need for importing Path from the hadoop java API
    */
  type Path = org.apache.hadoop.fs.Path
}