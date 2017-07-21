package fr.polytechnique.cmap.cnam.etl.config

trait CaseClassConfig {

  /**
    * Returns a string to allow pretty-printing the fields of a case class
    */
  def toPrettyString: String = {
    this.getClass.getDeclaredFields.collect {
      case f if f.getName.head != '$' =>
        f.setAccessible(true)
        s"  ${f.getName.trim} -> ${f.get(this).toString.trim}"
    }.mkString("{\n", "\n", "\n}").trim
  }
}
