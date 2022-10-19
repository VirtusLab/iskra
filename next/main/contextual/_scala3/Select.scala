//> using target.scala "3"

package org.virtuslab.iskra
package contextual

import org.virtuslab.iskra.frame.SchemaView

class Select[View <: SchemaView](val view: View, val underlying: UntypedDataFrame)

object Select:
  extension [View <: SchemaView](select: Select[View])
    transparent inline def apply[Columns](columns: View ?=> Columns): DataFrame[?] =
      ${ SelectImpl.applyImpl[View, Columns]('select, 'columns) }