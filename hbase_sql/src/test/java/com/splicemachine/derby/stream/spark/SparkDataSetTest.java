/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.stream.spark;

import com.clearspring.analytics.util.Lists;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.SQLDouble;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.stream.AbstractDataSetTest;
import com.splicemachine.derby.stream.function.LocatedRowToRowFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.spark.SparkDataSet;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Ignore;
import org.junit.Test;
import static org.apache.spark.sql.functions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by jleach on 4/15/15.
 */
@Ignore
public class SparkDataSetTest extends AbstractDataSetTest{

    public SparkDataSetTest() {
        super();
    }

    @Override
    protected DataSet<ExecRow> getTenRowsTwoDuplicateRecordsDataSet() {
        return new SparkDataSet<>(SpliceSpark.getContext().parallelize(tenRowsTwoDuplicateRecords));
    }
// Supported join types include: 'inner', 'outer', 'full', 'fullouter', 'leftouter', 'left', 'rightouter', 'right', 'leftsemi', 'leftanti'
    @Test
    public void testFoobar() {
        List<Row> foo = new ArrayList();
        for (int i = 0; i< 10; i++) {
            ValueRow row = new ValueRow(1);
            row.setColumn(1,new SQLInteger(i));
            foo.add(row);
        }

        StructType schema = DataTypes.createStructType(new StructField[]{DataTypes.createStructField("col1", DataTypes.IntegerType, true)});

//        ValueRow row = new ValueRow(2);
//        row.setColumn(1,new SQLDouble());
//        row.setColumn(2,new SQLInteger());

/*

        SpliceSpark.getSession().read().parquet("/Users/jleach/Documents/workspace/spliceengine/hbase_sql/target/external/simple_parquet")
                .select(new Column("0"),new Column("1"))
                .filter(col("0").gt(1).or(col("0").lt(4))).explain(true);
*/
        SpliceSpark.getSession().createDataFrame(foo,schema).write().format("orc").mode(SaveMode.Append)
                .orc("/Users/jleach/Documents/workspace/spliceengine/hbase_sql/target/external/orc_it");

        Column filter = (new Column("col1")).gt(1l).and(new Column("col1").lt(1l));

        SpliceSpark.getSession().read().schema(schema)
                .orc("/Users/jleach/Documents/workspace/spliceengine/hbase_sql/target/external/orc_it")
                .filter(filter).show();
//                .select(new Column("0"),new Column("1")).show();

/*
        Dataset<Row> leftSide = SpliceSpark.getSession().createDataFrame(foo,foo.get(0).schema());
        Dataset<Row> rightSide = SpliceSpark.getSession().createDataFrame(foo.subList(0,8),foo.get(0).schema());

        Column col =
                (leftSide.col("0").equalTo(rightSide.col("0"))).
                and((leftSide.col("1")).equalTo(rightSide.col("1")));
        leftSide.join(rightSide,col,"inner").explain(true);
        leftSide.join(rightSide,col,"inner").show(10);
        leftSide.join(broadcast(rightSide),col,"leftouter").explain(true);
        leftSide.join(broadcast(rightSide),col,"leftouter").show(10);
        leftSide.join(broadcast(rightSide),col,"leftanti").show(10);
        */
    }

}