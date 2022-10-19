//> using target.scala "2"

package org.virtuslab.iskra
package frame

class DataFrameMacroOps[S <: StructSchema](df: DataFrame[S]) {
  // inline def collectAs[A]: List[A] =
  //   ${ collectAsImpl[S, A]('df) }
}
