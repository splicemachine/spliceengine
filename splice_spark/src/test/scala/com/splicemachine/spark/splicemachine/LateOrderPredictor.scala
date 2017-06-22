package com.splicemachine.spark.splicemachine

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}

/**
  * Created by admin on 6/20/17.
  */
class LateOrderPredictor extends Timeline{


  def learnModel(): Unit = {

    // Add a label to the transfer order events which is how late it is and
    // grab every transferOrder that did not have an event and insert to the events with a 0 lateness
//   val df = assembleFeatures()
    val optionMap = Map(
      JDBCOptions.JDBC_TABLE_NAME -> TOTable,
      JDBCOptions.JDBC_URL -> defaultJDBCURL
    )
    val df = sqlContext.read.options(optionMap).splicemachine

    df.printSchema()

    //assemble feature vector from dataframe
    val assembler = new VectorAssembler()
      .setInputCols(Array("SHIPFROM", "SHIPTO", "SOURCEINVENTORY", "DESTINATIONINVENTORY", "SUPPLIER", "TRANSPORTMODE", "CARRIER", "FROMWEATHER", "TOWEATHER"))
      .setOutputCol("FEATURES")

    val output = assembler.transform(df)
    println("Assembled columns ShipFrom, ShipTo, SourceInventory, DestinationInventory, Supplier, TransportMode, Carrier, FromWeather, ToWeather to vector column 'features'")
    output.select("FEATURES", "LATENESS").show(false)

    // Set parameters for the algorithm.
    // Here, we limit the number of iterations to 10.
    val lr = new LogisticRegression().setMaxIter(10)

    // Fit the model to the data.
    val model = lr.fit(output)

    // Given a dataset, predict each point's label, and show the results.
    model.transform(output).show()

  }

  test("Vector Assembler") {
    learnModel()
    assert(true)
  }


/*

CREATE TABLE TIMELINE.FEATURES AS
SELECT
    TimeLine.TO_DELIVERY_CHG_EVENT.orgdeliverydate,
    TimeLine.TO_DELIVERY_CHG_EVENT.newdeliverydate,
    CASE WHEN TimeLine.TO_DELIVERY_CHG_EVENT.TO_EVENT_ID is Null
        THEN TimeLine.TransferOrders.fromweather
        ELSE TimeLine.TO_DELIVERY_CHG_EVENT.fromweather end as currentweather,
    TimeLine.TransferOrders.*,
    CASE WHEN TimeLine.TO_DELIVERY_CHG_EVENT.TO_EVENT_ID is Null
        THEN 0
        ELSE TimeLine.TO_DELIVERY_CHG_EVENT.newdeliverydate - TimeLine.TO_DELIVERY_CHG_EVENT.orgdeliverydate end as lateness,
    CASE
    WHEN  TimeLine.TO_DELIVERY_CHG_EVENT.newdeliverydate - TimeLine.TO_DELIVERY_CHG_EVENT.orgdeliverydate > 0
    THEN
        CASE
            WHEN  TimeLine.TO_DELIVERY_CHG_EVENT.newdeliverydate - TimeLine.TO_DELIVERY_CHG_EVENT.orgdeliverydate > 5
            THEN
                CASE
                    WHEN  TimeLine.TO_DELIVERY_CHG_EVENT.newdeliverydate - TimeLine.TO_DELIVERY_CHG_EVENT.orgdeliverydate > 10
                    THEN 3
                    ELSE 2
                END
            ELSE 1

        END
    ELSE 0
    END AS label

 from TimeLine.TransferOrders Left Outer Join TimeLine.TO_DELIVERY_CHG_EVENT
 on TimeLine.TransferOrders.TO_ID = TimeLine.TO_DELIVERY_CHG_EVENT.TO_ID


 */

  def assembleFeatures(): DataFrame = {
    val optionMap = Map(
      JDBCOptions.JDBC_TABLE_NAME -> TOTable,
      JDBCOptions.JDBC_URL -> defaultJDBCURL
    )
    val JDBCOps = new JDBCOptions(optionMap)
    val conn = JdbcUtils.createConnectionFactory(JDBCOps)()
    val joinCollumns =
      TOTable + ".*, " +
        s"case when $TOETable" + s".TO_EVENT_ID is Null then $TOTable" + s".fromweather else $TOETable" + ".fromweather end as currentweather, " +
        s"case when $TOETable" + s".TO_EVENT_ID is Null then $TOTable" + s".deliverydate else $TOETable" + ".newdeliverydate end as targetdate, " +
        s"case when $TOETable" + s".TO_EVENT_ID is Null then 0 else $TOETable" + s".newdeliverydate - $TOTable" + ".deliverydate end as lateness "
    val stmt = s"create table $FeatureTable as select $joinCollumns from $TOTable Left Outer Join $TOETable on " +
      TOTable + ".TO_ID = " + TOETable + ".TO_ID"
    println("query=" + stmt)
    conn.createStatement().execute(stmt)
    val features = Map(
      JDBCOptions.JDBC_TABLE_NAME -> FeatureTable,
      JDBCOptions.JDBC_URL -> defaultJDBCURL
    )
    val df = sqlContext.read.options(features).splicemachine
    df
  }

    ignore("Feature Engineering") {
      val df = assembleFeatures
      assert(df.count > 0)
    }

}
