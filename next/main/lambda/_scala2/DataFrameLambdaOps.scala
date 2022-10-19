//> using target.scala "2"

package org.virtuslab.iskra
package lambda

class DataFrameLambdaOps[Schema <: StructSchema](df: DataFrame[Schema]) {
  //transparent inline def select: Select[?] = ${ SelectImpl.selectImpl[Schema]('{df}) }
}