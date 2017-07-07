package com.splicemachine.spark.splicemachine

import java.sql.{Connection, Timestamp}

import com.splicemachine.si.api.txn.WriteConflict
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
/**
  * Created by MZweben on 6/20/17.
  */
class LateOrderPredictor extends Timeline {

  def whatif(source: Integer,
             destination: Integer,
             shippingDate: Timestamp,
             deliveryDate: Timestamp,
             newDeliveryDate: Timestamp,
             qty: Long,
             retryCount: Integer = 0,
             revertFlag: Boolean): Unit = {
    val conn: Connection = splicemachineContext.getConnection()
    var qtyMultiplier = 1 // change to -1 when reverting
    try {
      if (revertFlag) {
        qtyMultiplier = -1
      } // used to undo the what-if by inverting qty
      conn.setAutoCommit(true) //TBD - Need to set to false when DBAAS-570 is resolved
      update(InventoryTable, source, shippingDate, deliveryDate, qtyMultiplier * qty, CHANGE_AT_ST)
      update(InventoryTable, destination, shippingDate, deliveryDate, qtyMultiplier * -qty, CHANGE_AT_ET)
      update(InventoryTable, source, shippingDate, newDeliveryDate, qtyMultiplier * -qty, CHANGE_AT_ST)
      update(InventoryTable, destination, shippingDate, newDeliveryDate, qtyMultiplier * qty, CHANGE_AT_ET)
      conn.commit()
    }
    catch {
      case exp: WriteConflict => {
        conn.rollback()
        conn.setAutoCommit(true)
        if (retryCount < MAX_RETRIES) {
          println("Retrying create TO" + source + " " + destination + " " + shippingDate + " " + deliveryDate + " " + qty + " " + retryCount + 1)
          whatif(source, destination, shippingDate, deliveryDate, newDeliveryDate, qty, retryCount + 1, revertFlag)
        }
        else {
          // put code here to handle too many retries
        }
      }
      case e: Throwable => println(s"Got some other kind of exception: $e")
    }
    finally {
      conn.setAutoCommit(true)
    }
  }

  def simulateLateOrder(orderid: BigInt): Unit = {

    // Will need to copy the inventory table so that what-if is not visible to others

    val transferOrdersTable = Map(
      JDBCOptions.JDBC_TABLE_NAME -> "Timeline.TransferOrders",
      JDBCOptions.JDBC_URL -> defaultJDBCURL
    )
    val q = s"select *  FROM timeline.transferorders WHERE to_id = $orderid"
    val order = splicemachineContext.df(q)
    val days: Long = 5
    val source = order.first().getAs("SOURCEINVENTORY").asInstanceOf[Long]
    val dest = order.first().getAs("DESTINATIONINVENTORY").asInstanceOf[Long]
    val ship = order.first().getAs("SHIPDATE").asInstanceOf[Timestamp]
    val delivery = order.first().getAs("DELIVERYDATE").asInstanceOf[Timestamp]
    val qty = order.first().getAs("QTY").asInstanceOf[Long]
    val delta: Long = days * 24 * 60 * 60 * 1000
    val newDelivery = new Timestamp(delivery.getTime + delta) // TBD: use Joda
    whatif(source.toInt, dest.toInt, ship, delivery, newDelivery, qty, 5, false)
    val destInvCol = TOTable + ".destinationinventory "
    val timelineIdCol = InventoryTable + ".timeline_id "
    val toIdCol = TOTable + ".to_id "
    val stCol = InventoryTable + ".ST "
    val delDateCol = TOTable + ".deliverydate "
    val shipDateCol = TOTable + ".shipdate "
    val sourceInvCol = TOTable + ".sourceinventory "
    val latCol = TOTable + ".latitude "
    val longCol = TOTable + ".longitude "
    val srcWeatherCol = TOTable + ".fromweather "
    val destWeatherCol = TOTable + ".toweather "
    val query =
      s"""SELECT $toIdCol, $stCol, $timelineIdCol FROM $InventoryTable , $TOTable
                        WHERE $timelineIdCol = $dest
                        AND $destInvCol = $timelineIdCol
                        AND val < 0
                        AND $stCol >= $delDateCol
                        ORDER BY $stCol"""
    println(s"q=$query")
    val stockOuts = splicemachineContext.df(query)
    whatif(source.toInt, dest.toInt, ship, delivery, newDelivery, qty, 5, true) // undo what-if
    splicemachineContext.insert(stockOuts, stockoutTable)
  }
/*
  test("Simulate Test"){
    ATPquery = s"""select VAL AS Available from timeline.timeline_int
                      where timeline_id = ${inv= 100}
                      AND ST <= TIMESTAMP('2017-05-05 00:00:00.0')
                      AND ET > TIMESTAMP('2017-05-05 00:00:00.0')
    simulateLateOrder(22)
    asseert

  }
*/


  def binLabels(begin: String, end:String): Unit = {
    val binQuery = s"""CREATE table TIMELINE.FEATURES AS
                            SELECT
                                TimeLine.TO_DELIVERY_CHG_EVENT.orgdeliverydate,
                                TimeLine.TO_DELIVERY_CHG_EVENT.newdeliverydate,
                                CASE WHEN TimeLine.TO_DELIVERY_CHG_EVENT.TO_EVENT_ID is Null
                                    THEN TimeLine.TransferOrders.fromweather
                                    ELSE TimeLine.TO_DELIVERY_CHG_EVENT.fromweather end as currentweather,
                                TimeLine.TransferOrders.*,
                                CASE WHEN TimeLine.TO_DELIVERY_CHG_EVENT.TO_EVENT_ID is Null
                                    THEN 0
                                    ELSE TimeLine.TO_DELIVERY_CHG_EVENT.newdeliverydate - TimeLine.TO_DELIVERY_CHG_EVENT.orgdeliverydate end as Lateness,
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
                                END AS Label
                             from TimeLine.TransferOrders Left Outer Join TimeLine.TO_DELIVERY_CHG_EVENT
                             on TimeLine.TransferOrders.TO_ID = TimeLine.TO_DELIVERY_CHG_EVENT.TO_ID
                             WHERE  TIMESTAMP('$begin') >= timeline.transferorders.deliverydate
                            AND TIMESTAMP('$end') > timeline.transferorders.deliverydate"""
    splicemachineContext.df(binQuery)
    }

  def learn(): Unit = {

    val optionMap = Map(
      JDBCOptions.JDBC_TABLE_NAME -> "Timeline.Features",
      JDBCOptions.JDBC_URL -> defaultJDBCURL
    )
    val dfUpper = sqlContext.read.options(optionMap).splicemachine
    val newNames = Seq("orgdeliverydate", "newdeliverydate", "currentweather", "to_id",
      "po_id", "shipfrom", "shipto", "shipdate", "deliverydate", "moddeliverydate", "sourceinventory",
      "destinationinventory", "qty", "supplier", "asn", "container", "transportmode", "carrier",
      "fromweather", "toweather", "latitude", "longitude", "lateness", "label")
    val df = dfUpper.toDF(newNames: _*)


    //assemble feature vector from dataframe
    val assembler = new VectorAssembler()
      .setInputCols(Array("shipfrom", "shipto", "sourceinventory", "destinationinventory", "supplier", "transportmode", "carrier", "fromweather", "toweather"))
      .setOutputCol("features")

    val output = assembler.transform(df)
    println("Assembled columns ShipFrom, ShipTo, SourceInventory, DestinationInventory, Supplier, TransportMode, Carrier, FromWeather, ToWeather to vector column 'features'")
    output.select("features", "label").show(true)

    // Set parameters for the algorithm.
    // Here, we limit the number of iterations to 10.
    val lr = new LogisticRegression()
      .setMaxIter(10)

    // Fit the model to the data.
    val model = lr.fit(output)

    // Given a dataset, predict each point's label, and show the results.
    val newdf = model.transform(output)

    // Print the coefficients and intercept for multinomial logistic regression
    println(s"Coefficients: \n${model.coefficientMatrix}")
    println(s"Intercepts: ${model.interceptVector}")

  }

}
