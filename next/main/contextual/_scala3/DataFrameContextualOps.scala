//> using target.scala "3"

package org.virtuslab.iskra
package contextual

import scala.quoted.*

class DataFrameContextualOps[Schema <: StructSchema](df: DataFrame[Schema]) {
  transparent inline def select: Select[?] = ${ SelectImpl.selectImpl[Schema]('{df}) }
}