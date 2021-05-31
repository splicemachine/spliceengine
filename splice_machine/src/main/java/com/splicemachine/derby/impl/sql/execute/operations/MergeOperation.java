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
import com.splicemachine.db.impl.sql.compile.MatchingClauseNode;
import com.splicemachine.db.impl.sql.execute.TemporaryRowHolderImpl;
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

    private TemporaryRowHolderImpl[]    _thenRows;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

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
        _thenRows = new TemporaryRowHolderImpl[ _constants.matchingClauseCount() ];
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
        if ( !rowsFound )
        {
            activation.addWarning( StandardException.newWarning( SQLState.LANG_NO_ROW_FOUND ) );
        }

        // now execute the INSERT/UPDATE/DELETE actions
        int         clauseCount = _constants.matchingClauseCount();
        for ( int i = 0; i < clauseCount; i++ )
        {
            _constants.getMatchingClause( i ).executeConstantAction( activation, _thenRows[ i ] );
        }

        cleanUp();
        //endTime = getCurrentTimeMillis();
    }

    @Override
    public void setup() throws StandardException
    {
        super.setup();

        int         clauseCount = _constants.matchingClauseCount();
        for ( int i = 0; i < clauseCount; i++ )
        {
            _constants.getMatchingClause( i ).init();
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
        int         clauseCount = _constants.matchingClauseCount();
        for ( int i = 0; i < clauseCount; i++ )
        {
            TemporaryRowHolderImpl  thenRows = _thenRows[ i ];
            if ( thenRows != null )
            {
                thenRows.close();
                _thenRows[ i ] = null;
            }

            _constants.getMatchingClause( i ).cleanUp();
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
        DataValueDescriptor     rlColumn;
        RowLocation             baseRowLocation;
        boolean rowsFound = false;

        while ( true )
        {
            // may need to objectify stream columns here.
            // see DMLWriteResultSet.getNextRowCoure(NoPutResultSet)
            _row =  _drivingLeftJoin.getNextRowCore();
            if ( _row == null ) { break; }

            // By convention, the last column for the driving left join contains a data value
            // containing the RowLocation of the target row.

            rowsFound = true;

            rlColumn = _row.getColumn( _row.nColumns() );

            // todo: not clear what this does
            baseRowLocation = null;

            boolean matched = false;
            if ( rlColumn != null )
            {
                if ( !rlColumn.isNull() )
                {
                    matched = true;
                    baseRowLocation = (RowLocation) rlColumn; //.getObject(); // todo what is this
                }
            }

            // find the first clause which applies to this row
            MatchingClauseConstantAction    matchingClause = null;
            int         clauseCount = _constants.matchingClauseCount();
            int         clauseIdx = 0;
            for ( ; clauseIdx < clauseCount; clauseIdx++ )
            {
                MatchingClauseConstantAction    candidate = _constants.getMatchingClause( clauseIdx );
                boolean isWhenMatchedClause = false;

                switch ( candidate.clauseType() )
                {
                    case MatchingClauseNode.WHEN_MATCHED_THEN_UPDATE:
                    case MatchingClauseNode.WHEN_MATCHED_THEN_DELETE:
                        isWhenMatchedClause = true;
                        break;
                }

                boolean considerClause = (matched == isWhenMatchedClause);

                if ( considerClause )
                {
                    if ( candidate.evaluateRefinementClause( activation ) )
                    {
                        matchingClause = candidate;
                        break;
                    }
                }
            }

            if ( matchingClause != null )
            {
                _thenRows[ clauseIdx ] = matchingClause.bufferThenRow( activation, _thenRows[ clauseIdx ], _row );
                _rowCount++;
            }
        }

        return rowsFound;
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
}