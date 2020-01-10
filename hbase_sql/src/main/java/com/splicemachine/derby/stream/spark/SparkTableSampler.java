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

package com.splicemachine.derby.stream.spark;

import com.clearspring.analytics.util.Lists;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.services.property.PropertyUtil;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.stats.ColumnStatisticsImpl;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.stream.function.BulkLoadKVPairFunction;
import com.splicemachine.derby.stream.function.IndexTransformFunction;
import com.splicemachine.derby.stream.function.RowKeyStatisticsFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.TableSampler;
import com.splicemachine.derby.stream.utils.BulkLoadUtils;
import com.splicemachine.si.api.txn.TxnView;
import scala.Tuple2;

import java.util.List;

/**
 * Created by jyuan on 10/9/18.
 */
public class SparkTableSampler implements TableSampler {

    private DataSet dataSet;
    private DDLMessage.TentativeIndex tentativeIndex;
    private String indexName;
    private double sampleFraction;
    private OperationContext operationContext;

    public SparkTableSampler(OperationContext operationContext,
                             DataSet dataSet,
                             DDLMessage.TentativeIndex tentativeIndex,
                             String indexName,
                             double sampleFraction) {
        this.operationContext = operationContext;
        this.dataSet = dataSet;
        this.tentativeIndex = tentativeIndex;
        this.indexName = indexName;
        this.sampleFraction = sampleFraction;
    }


    @Override
    public byte[][] sample() throws StandardException {
        if (sampleFraction == 0) {
            Activation activation = operationContext.getActivation();
            LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
            sampleFraction = BulkLoadUtils.getSampleFraction(lcc);
        }
        DataSet sampledDataSet = dataSet.sampleWithoutReplacement(sampleFraction);
        DataSet sampleRowAndIndexes = sampledDataSet
                .map(new IndexTransformFunction(tentativeIndex), null, false, true,
                        String.format("Create Index %s: Sample Data", indexName))
                .mapPartitions(new BulkLoadKVPairFunction(-1), false, true,
                        String.format("Create Index %s: Sample Data", indexName));

        // collect statistics for encoded key/value, include size and histgram
        RowKeyStatisticsFunction statisticsFunction =
                new RowKeyStatisticsFunction(-1, Lists.newArrayList());
        DataSet keyStatistics = sampleRowAndIndexes.mapPartitions(statisticsFunction);

        List<Tuple2<Long, Tuple2<Double, ColumnStatisticsImpl>>> result = keyStatistics.collect();

        // Calculate cut points for main table and index tables
        List<Tuple2<Long, byte[][]>> cutPoints = BulkLoadUtils.getCutPoints(sampleFraction, result);


        if (cutPoints.size() == 1)
            return cutPoints.get(0)._2;
        else if (cutPoints.size() == 0)
            return null;
        else
            throw new RuntimeException("Unexpected cutpoints");
    }
}
