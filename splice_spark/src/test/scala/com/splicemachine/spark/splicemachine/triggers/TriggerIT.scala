/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */
package com.splicemachine.spark.splicemachine.triggers

import org.apache.spark.sql.Row
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class TriggerIT extends FunSuite with TestContext with Matchers {

  test("Test Insert with Trigger") {  // Added for DB-10707
    import org.apache.spark.sql.types._

    val sch = StructType(
      StructField("I", IntegerType, false) ::
      StructField("C", StringType, true) :: Nil
    )

    val t1 = s"$schema.foo"
    val t2 = s"$schema.bar"
    splicemachineContext.createTable(t1, sch, Seq("I") )
    splicemachineContext.createTable(t2, sch, Seq("I") )

    execute(s"""CREATE TRIGGER FOO_TRIGGER
               |AFTER INSERT ON $t1
               |REFERENCING NEW_TABLE AS NEW_ROWS
               |INSERT INTO $t2 (i,c) SELECT i,c FROM NEW_ROWS""".stripMargin)

    splicemachineContext.insert(
      dataframe(
        rdd( Seq(
          Row.fromSeq( Seq(1,"one") ),
          Row.fromSeq( Seq(2,"two") ),
          Row.fromSeq( Seq(3,"three") )
        ) ),
        sch
      ),
      t1
    )

    def res: String => String = table => executeQuery(
      s"select * from $table",
      rs => {
        var s = Seq.empty[String]
        while( rs.next ) {
          s = s :+ s"${rs.getInt(1)},${rs.getString(2)}"
        }
        s.sorted.mkString("; ")
      }
    ).asInstanceOf[String]

    val res1 = res(t1)
    val res2 = res(t2)

    splicemachineContext.dropTable(t1)
    splicemachineContext.dropTable(t2)

    org.junit.Assert.assertEquals(res1, res2)
  }

}
