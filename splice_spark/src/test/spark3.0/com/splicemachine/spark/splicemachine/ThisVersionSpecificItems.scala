package com.splicemachine.spark.splicemachine

import org.apache.spark.sql.SparkSession

object ThisVersionSpecificItems {

  def beforeAll(spark: SparkSession): Unit = {
    spark.conf.set("spark.sql.legacy.setCommandRejectsSparkCoreConfs", "false")  // enables setting spark.ui.enabled to false
  }

  val schema = SparkVersionSpecificItems.schemaWithoutMetadata
}
