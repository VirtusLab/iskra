//> using target.scala "3"

package org.virtuslab.iskra
package lambda

import scala.quoted.*

class DataFrameLambdaOps[Schema <: StructSchema](df: DataFrame[Schema]) {
  // transparent inline def select: Select[?] = ${ SelectImpl.selectImpl[Schema]('{df}) }
}