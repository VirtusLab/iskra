//> using target.scala "3"

package org.virtuslab.iskra
package frame

class DataFrameMacroOps[Schema <: StructSchema](df: DataFrame[Schema]) {
  inline def collectAs[A]: List[A] =
    ${ CollectAs.collectAsImpl[Schema, A]('df) }
}
