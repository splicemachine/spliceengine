/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.execute;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.Storable;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.execute.ExecAggregator;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.UserDataValue;

/**
 * Adaptor that sits between execution layer and aggregates.
 */
public class GenericAggregator {
    private final AggregatorInfo aggInfo;
    int aggregatorColumnId;
    private int inputColumnId;
    private int resultColumnId;

    private final ClassFactory cf;

    /*
     ** We cache an aggregator to speed up
     ** the instantiation of lots of aggregators.
     */
    private ExecAggregator cachedAggregator;

    /**
     * Constructor:
     *
     * @param aggInfo information about the user aggregate
     * @param cf      the class factory.
     */
    public GenericAggregator
    (
            AggregatorInfo aggInfo,
            ClassFactory cf
    ) {
        this.aggInfo = aggInfo;
        aggregatorColumnId = aggInfo.getAggregatorColNum();
        inputColumnId = aggInfo.getInputColNum();
        resultColumnId = aggInfo.getOutputColNum();
        this.cf = cf;
    }


    /**
     * Initialize the aggregator
     *
     * @throws StandardException on error
     * @param    row the row with the aggregator to be initialized
     */
    void initialize(ExecRow row)
            throws StandardException {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(row != null, "row is null");
        }

        UserDataValue aggregatorColumn = (UserDataValue) row.getColumn(aggregatorColumnId + 1);

        ExecAggregator ua = (ExecAggregator) aggregatorColumn.getObject();
        if (ua == null) {
            ua = getAggregatorInstance();
            aggregatorColumn.setValue(ua);
        }
    }

    /**
     * Accumulate the aggregate results.  This is the
     * guts of the aggregation.  We will call the user aggregate
     * on itself to do the aggregation.
     *
     * @throws StandardException on error
     * @param    inputRow the row with the input colum
     * @param    accumulateRow the row with the aggregator
     */
    void accumulate(ExecRow inputRow,
                    ExecRow accumulateRow)
            throws StandardException {
        DataValueDescriptor inputColumn = null;

        if (SanityManager.DEBUG) {
            SanityManager.ASSERT((inputRow != null) && (accumulateRow != null), "bad accumulate call");
        }

        DataValueDescriptor aggregatorColumn = accumulateRow.getColumn(aggregatorColumnId + 1);

        inputColumn = inputRow.getColumn(inputColumnId + 1);

        accumulate(inputColumn, aggregatorColumn);
    }

    /**
     * Accumulate the aggregate results.  This is the
     * guts of the aggregation.  We will call the user aggregate
     * on itself to do the aggregation.
     *
     * @throws StandardException on error
     * @param    inputRow the row with the input colum
     * @param    accumulateRow the row with the aggregator
     */
    void accumulate(Object[] inputRow,
                    Object[] accumulateRow)
            throws StandardException {
        DataValueDescriptor inputColumn = null;

        if (SanityManager.DEBUG) {
            SanityManager.ASSERT((inputRow != null) && (accumulateRow != null), "bad accumulate call");
        }

        DataValueDescriptor aggregatorColumn = (DataValueDescriptor) accumulateRow[aggregatorColumnId];
        inputColumn = (DataValueDescriptor) inputRow[inputColumnId];

        accumulate(inputColumn, aggregatorColumn);
    }

    /**
     * Accumulate the aggregate results.  This is the
     * guts of the aggregation.  We will call the user aggregate
     * on itself to do the aggregation.
     *
     * @throws StandardException on error
     * @param    inputColumn
     * @param    aggregatorColumn
     */
    void accumulate(DataValueDescriptor inputColumn,
                    DataValueDescriptor aggregatorColumn)
            throws StandardException {
        ExecAggregator ua;

        if (SanityManager.DEBUG) {
            /*
             ** Just to be on the safe side, confirm that we actually
             ** have a Aggregator in this column.
             */
            if (!(aggregatorColumn instanceof UserDataValue)) {
                SanityManager.THROWASSERT("accumlator column is not a UserDataValue as " +
                        "expected, it is a " + aggregatorColumn.getClass().getName());
            }
        }
        ua = (ExecAggregator) aggregatorColumn.getObject();

        /*
         ** If we don't have an aggregator, then we have to
         ** create one now.  This happens when the input result
         ** set is null.
         */
        if (ua == null) {
            ua = getAggregatorInstance();
        }

        ua.accumulate(inputColumn, this);
    }

    /**
     * Merge the aggregate results.  This is the
     * guts of the aggregation.  We will call the user aggregate
     * on itself to do the aggregation.
     *
     * @throws StandardException on error
     * @param    inputRow the row with the input colum
     * @param    mergeRow the row with the aggregator
     */
    void merge(ExecRow inputRow,
               ExecRow mergeRow)
            throws StandardException {

        DataValueDescriptor mergeColumn = mergeRow.getColumn(aggregatorColumnId + 1);
        DataValueDescriptor inputColumn = inputRow.getColumn(aggregatorColumnId + 1);

        merge(inputColumn, mergeColumn);
    }

    /**
     * Merge the aggregate results.  This is the
     * guts of the aggregation.  We will call the user aggregate
     * on itself to do the aggregation.
     *
     * @throws StandardException on error
     * @param    inputRow the row with the input colum
     * @param    mergeRow the row with the aggregator
     */
    void merge(Object[] inputRow,
               Object[] mergeRow)
            throws StandardException {
        DataValueDescriptor mergeColumn = (DataValueDescriptor) mergeRow[aggregatorColumnId];
        DataValueDescriptor inputColumn = (DataValueDescriptor) inputRow[aggregatorColumnId];

        merge(inputColumn, mergeColumn);
    }

    /**
     * Get the results of the aggregation and put it
     * in the result column.
     *
     * @throws StandardException on error
     * @param    row    the row with the result and the aggregator
     */
    public boolean finish(ExecRow row)
            throws StandardException {
        DataValueDescriptor outputColumn = row.getColumn(resultColumnId + 1);
        DataValueDescriptor aggregatorColumn = row.getColumn(aggregatorColumnId + 1);
        /*
         ** Just to be on the safe side, confirm that we actually
         ** have a Aggregator in aggregatorColumn.
         */
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(aggregatorColumn != null, "aggregatorColumn is null");
            SanityManager.ASSERT(outputColumn != null, "otuputColumn is null");
            SanityManager.ASSERT(aggregatorColumn instanceof UserDataValue,
                    "accumlator column is not a UserDataValue as expected");
        }

        ExecAggregator ua = (ExecAggregator) aggregatorColumn.getObject();

        /*
         ** If we don't have an aggregator, then we have to
         ** create one now.  This happens when the input result
         ** set is null.
         */
        if (ua == null) {
            ua = getAggregatorInstance();
        }

        /*
         **
         ** We are going to copy
         ** then entire DataValueDescriptor into the result column.
         ** We could call setValue(result.setObject()), but we
         ** might loose state (e.g. SQLBit.getObject() returns a
         ** byte[] which looses the precision of the bit.
         **
         */

        DataValueDescriptor result = ua.getResult();
        if (result == null)
            outputColumn.setToNull();
        else
            outputColumn.setValue(result);

        return ua.didEliminateNulls();
    }

    /**
     * Get a new instance of the aggregator and initialize it.
     *
     * @return an exec aggregator
     * @throws StandardException on error
     */
    ExecAggregator getAggregatorInstance()
            throws StandardException {
        ExecAggregator aggregatorInstance;
        if (cachedAggregator == null) {
            try {
                Class aggregatorClass = cf.loadApplicationClass(aggInfo.getAggregatorClassName());
                Object agg = aggregatorClass.newInstance();
                aggregatorInstance = (ExecAggregator) agg;
                cachedAggregator = aggregatorInstance;
                aggregatorInstance.setup
                        (
                                cf,
                                aggInfo.getAggregateName(),
                                aggInfo.getResultDescription().getColumnInfo()[0].getType(),
                                aggInfo.getParam()
                        );

            } catch (Exception e) {
                throw StandardException.unexpectedUserException(e);
            }
        } else {
            aggregatorInstance = cachedAggregator.newAggregator();
        }


        return aggregatorInstance;
    }

    /////////////////////////////////////////////////////////////
    //
    /////////////////////////////////////////////////////////////

    /**
     * Return the column id that is being aggregated
     */
    int getColumnId() {
        // Every sort has to have at least one column.
        return aggregatorColumnId;
    }

    DataValueDescriptor getInputColumnValue(ExecRow row)
            throws StandardException {
        return row.getColumn(inputColumnId + 1);
    }

    /**
     * Merge two partial aggregations.  This is how the
     * sorter merges partial aggregates.
     *
     * @throws StandardException on error
     */
    void merge(Storable aggregatorColumnIn,
               Storable aggregatorColumnOut)
            throws StandardException {
        ExecAggregator uaIn;
        ExecAggregator uaOut;

        if (SanityManager.DEBUG) {
            /*
             ** Just to be on the safe side, confirm that we actually
             ** have a Aggregator in this column.
             */
            if (!(aggregatorColumnIn instanceof UserDataValue)) {
                SanityManager.THROWASSERT("aggregatorColumnOut column is not " +
                        "a UserAggreator as expected, " +
                        "it is a " + aggregatorColumnIn.getClass().getName());
            }
            if (!(aggregatorColumnOut instanceof UserDataValue)) {
                SanityManager.THROWASSERT("aggregatorColumnIn column is not" +
                        " a UserAggreator as expected, " +
                        "it is a " + aggregatorColumnOut.getClass().getName());
            }
        }
        uaIn = (ExecAggregator) (((UserDataValue) aggregatorColumnIn).getObject());
        uaOut = (ExecAggregator) (((UserDataValue) aggregatorColumnOut).getObject());

        uaOut.merge(uaIn);
    }

    //////////////////////////////////////////////////////
    //
    // MISC
    //
    //////////////////////////////////////////////////////
    AggregatorInfo getAggregatorInfo() {
        return aggInfo;
    }


}
