package com.splicemachine.spark.splicemachine

import org.apache.spark.sql.SparkSession

object ThisVersionSpecificItems {

  def beforeAll(spark: SparkSession): Unit = {}

  val schema = SparkVersionSpecificItems.schemaWithoutMetadata
}
