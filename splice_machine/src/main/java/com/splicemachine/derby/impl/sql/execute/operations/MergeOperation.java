/*
   Derby - Class org.apache.derby.impl.sql.execute.MergeResultSet
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
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

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private NoPutResultSet      _drivingLeftJoin;
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
        _drivingLeftJoin = drivingLeftJoin;
        _constants = (MergeConstantAction) activation.getConstantAction();
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

//    @Override
//    public final long modifiedRowCount() {
//        // todo, see SpliceBaseOperation.computeModifiedRows()
//        // return _rowCount + RowUtil.getRowCountBase();
//        return 0;
//    }

    public void open() throws StandardException
    {
        setup();

        boolean rowsFound = collectAffectedRows();
        if ( !rowsFound ) {
            activation.addWarning( StandardException.newWarning( SQLState.LANG_NO_ROW_FOUND ) );
        }

        // now execute the INSERT/UPDATE/DELETE actions
        for (MatchingClauseConstantAction clause : _constants.getMatchingClauses()) {
            clause.executeConstantAction(activation);
        }

        cleanUp();
        //endTime = getCurrentTimeMillis();
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
     * <p>
     * Loop through the rows in the driving left join.
     * </p>
     */
    boolean  collectAffectedRows() throws StandardException
    {
        boolean rowsFound = false;
        while ( true )
        {
            // may need to objectify stream columns here.
            // see DMLWriteResultSet.getNextRowCoure(NoPutResultSet)
            _row =  _drivingLeftJoin.getNextRowCore();
            if ( _row == null ) { break; }

            rowsFound = true;

            boolean matched = rowLocationColumnIsNotNull(_row);

            // find the first (!) clause which applies to this row
            for (MatchingClauseConstantAction clause : _constants.getMatchingClauses()) {
                // if we have a match, consider a clause if it is WHEN MATCHED THEN xxx
                // else (we have a non-match), consider a clause if it's not a WHEN MATCHED (e.g. WHEN NOT MATCHED THEN INSERT)
                if(matched != clause.isWhenMatchedClause())
                    continue;

                // check if matching refinement is satisfied
                if ( clause.satisfiesMatchingRefinement( activation ) )
                {
                    // buffer the row for later execution
                    clause.bufferThenRow( activation, _row );
                    _rowCount++;
                    // only execute first match/nonmatch
                    break;
                }
            }
        }

        return rowsFound;
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
        return null; // todo
    }

    @Override
    public boolean isReferencingTable(long tableNumber) {
        return false; // todo
    }

    @Override
    public String getName() {
        return null; // todo
    }

    @Override
    public int[] getRootAccessedCols(long tableNumber) throws StandardException {
        return new int[0]; // todo
    }
    @Override
    public boolean returnsRows() {
        return false;
    }

    @Override
    public long[] modifiedRowCount() {
        return new long[] { _rowCount };
    }
}