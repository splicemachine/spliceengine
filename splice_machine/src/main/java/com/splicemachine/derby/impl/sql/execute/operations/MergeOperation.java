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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.NoPutResultSet;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.iapi.types.SQLRef;
import com.splicemachine.derby.impl.sql.execute.actions.MatchingClauseConstantAction;
import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.derby.impl.sql.execute.actions.MergeConstantAction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import org.apache.commons.lang.NotImplementedException;

/**
 * INSERT/UPDATE/DELETE a target table based on how it outer joins
 * with a driving table. For a description of how Derby processes
 * the MERGE statement, see the header comment on MergeNode.
 */
// originally MergeResultSet extends NoRowsResultSetImpl
public class MergeOperation extends NoRowsOperation
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    protected static final String NAME = MergeOperation.class.getSimpleName().replaceAll("Operation","");

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private SpliceBaseOperation _drivingLeftJoin;
    private MergeConstantAction _constants;

    private ExecRow             _row;
    private long                _rowCount;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public MergeOperation() {} // needed for Externizable

    /**
     * Construct from a driving left join and an Activation.
     */
    public MergeOperation
    (
            NoPutResultSet drivingLeftJoin,
            Activation activation
    )
            throws StandardException
    {
        super( activation );
        _drivingLeftJoin = ((SpliceBaseOperation) drivingLeftJoin);
        _constants = (MergeConstantAction) activation.getConstantAction();
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public void open() throws StandardException
    {
        setup();
        loopThroughDrivingLeftJoin();
        cleanUp();
    }

    @Override
    public void setup() throws StandardException
    {
        super.setup();

        for (MatchingClauseConstantAction clause : _constants.getMatchingClauses()) {
            clause.init();
        }

        _rowCount = 0L;
        _drivingLeftJoin.openCore();
    }
    /**
     * Clean up resources and call close on data members.
     */
//    public void close() throws StandardException
//    {
//        //close( false ); // todo
//    }

    public void cleanUp() throws StandardException
    {
        for (MatchingClauseConstantAction clause : _constants.getMatchingClauses()) {
            clause.cleanUp();
        }
    }

    public void finish() throws StandardException
    {
        if ( _drivingLeftJoin != null ) { _drivingLeftJoin.finish(); }
        super.finish();
    }

    /**
     * For row in {driving left join}
     *      for matchingClause in matchingClausesList:
     *          if matchingClause.satisfiesMatchingRefinement(row)
     *
     * driving left join = SELECT selectList FROM sourceTable LEFT OUTER JOIN targetTable ON searchCondition
     */
    void loopThroughDrivingLeftJoin() throws StandardException
    {
        boolean rowsFound = false;
        while ( (_row = _drivingLeftJoin.getNextRowCore()) != null )
        {
            rowsFound = true;
            boolean matched = rowLocationColumnIsNotNull(_row);

            // find the first (!) clause which applies to this row
            for (MatchingClauseConstantAction clause : _constants.getMatchingClauses())
            {
                if( clause.consumeRow(activation, _row, matched) )
                {
                    _rowCount++;
                    // only execute first match/nonmatch
                    break;
                }
            }
        }

        // execute INSERT/UPDATE/DELETE actions with the rows that have been buffered for execution
        // (note part of this might have already executed in bufferThenRows, this is executing the "remainder"
        for (MatchingClauseConstantAction clause : _constants.getMatchingClauses()) {
            clause.executeConstantAction(activation);
        }

        if ( !rowsFound ) {
            activation.addWarning( StandardException.newWarning( SQLState.LANG_NO_ROW_FOUND ) );
        }
    }

    /**
     * By convention, the last column for the driving left join contains a data value
     * containing the RowLocation of the target row.
     * @return true if we have a valid row locaition
     * @throws StandardException
     */
    static private boolean rowLocationColumnIsNotNull(ExecRow row) throws StandardException {
        if( row.size() != row.nColumns() )
            throw new RuntimeException("Error in MergeOperation: not enough columns for RowLocation");
        DataValueDescriptor rlColumn = row.getColumn( row.nColumns() );
        if(rlColumn == null)
            return false;

        // todo: not clear why this is sometimes RowLocation, and sometimes SQLRef (then with RowLocation as getObject()).
        if( !(rlColumn instanceof RowLocation) && !(rlColumn instanceof SQLRef))
            throw new RuntimeException("Error in MergeOperation: last column type is " + rlColumn.getClass().getName());

        return !rlColumn.isNull();
    }


    ////////////////////////////////////////////////////////////////////////////////////////////////
    // to implement methods new in splicemachine

    @Override
    public DataSet<ExecRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        throw new NotImplementedException("MergeOperation.getDataSet");
    }

    @Override
    public boolean isReferencingTable(long tableNumber) {
        // todo: what else are we referencing? tables that are needed for triggers?
        return _drivingLeftJoin.isReferencingTable(tableNumber);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public int[] getRootAccessedCols(long tableNumber) throws StandardException {
        return _drivingLeftJoin.getRootAccessedCols(tableNumber);
    }

    @Override
    public boolean returnsRows() {
        return false;
    }

    /**
     * @return modified rows for "XXX rows inserted/updated/deleted"
     */
    @Override
    public long[] modifiedRowCount() {
        return new long[] { _rowCount };
    }
}