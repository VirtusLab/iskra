/*
Suggested usage in your REPL:

    import org.virtuslab.iskra.repl.*

Then you can simply create a data frame like

    val df = DF(1, 2, 3)

*/

package org.virtuslab.iskra.repl

import org.apache.spark.sql.SparkSession

// Not using `given` to make it accessible with a * wildcard import
implicit lazy val spark: SparkSession =
    SparkSession.builder().master("local").getOrCreate()

export org.virtuslab.iskra.api.*

export org.virtuslab.iskra.api.DataFrame as DF
