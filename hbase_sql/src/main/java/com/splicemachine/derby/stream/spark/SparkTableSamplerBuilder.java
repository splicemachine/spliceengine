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

import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.TableSampler;
import com.splicemachine.derby.stream.iapi.TableSamplerBuilder;
import com.splicemachine.si.api.txn.TxnView;

/**
 * Created by jyuan on 10/9/18.
 */
public class SparkTableSamplerBuilder implements TableSamplerBuilder{

    private DataSet dataSet;
    private DDLMessage.TentativeIndex tentativeIndex;
    private String indexName;
    private double sampleFraction;
    private OperationContext operationContext;

    public SparkTableSamplerBuilder(DataSet dataSet, OperationContext operationContext) {
        this.dataSet = dataSet;
        this.operationContext = operationContext;
    }

    @Override
    public TableSamplerBuilder tentativeIndex(DDLMessage.TentativeIndex tentativeIndex) {
        this.tentativeIndex = tentativeIndex;
        return this;
    }

    @Override
    public TableSamplerBuilder indexName(String indexName){
        this.indexName = indexName;
        return this;
    }

    @Override
    public TableSamplerBuilder sampleFraction(double sampleFraction){
        this.sampleFraction = sampleFraction;
        return this;
    }

    @Override
    public TableSampler build() {
        return new SparkTableSampler(operationContext, dataSet, tentativeIndex, indexName, sampleFraction);
    }
}
