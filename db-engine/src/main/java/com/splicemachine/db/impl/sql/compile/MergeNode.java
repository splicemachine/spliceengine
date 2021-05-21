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

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.sql.StatementType;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;

/**
 * This class represents a MERGE INTO statement.
 */
public class MergeNode extends DMLStatementNode
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  static  final   int SOURCE_TABLE_INDEX = 0;
    public  static  final   int TARGET_TABLE_INDEX = 1;

    private static  final   String  TARGET_ROW_LOCATION_NAME = "###TargetRowLocation";

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    //
    // Filled in by the constructor.
    //
    private FromBaseTable   _targetTable;
    private FromTable   _sourceTable;
    private ValueNode   _searchCondition;
    private QueryTreeNodeVector<MatchingClauseNode> _matchingClauses;

    //
    // Filled in at bind() time.
    //
    private ResultColumnList    _selectList;
    private FromList                _leftJoinFromList;
    private HalfOuterJoinNode   _hojn;

    //
    // Filled in at generate() time.
    //
    private ConstantAction      _constantAction;
    private CursorNode          _leftJoinCursor;

    public MergeNode(FromTable targetTable, FromTable sourceTable, ValueNode searchCondition,
                     QueryTreeNodeVector<MatchingClauseNode> matchingClauses, ContextManager cm)
            throws StandardException
    {
        setContextManager(cm);

//        if ( !( targetTable instanceof FromBaseTable) ) { notBaseTable(); }
//        else {
            _targetTable = (FromBaseTable) targetTable;
//        }

        _sourceTable = sourceTable;
        _searchCondition = searchCondition;
        _matchingClauses = matchingClauses;

        makeJoin();
    }

    /**
     * <p>
     * Construct the left outer join which will drive the execution.
     * </p>
     */
    private void    makeJoin() throws StandardException
    {
        resultSet = new HalfOuterJoinNode
                (
                        _sourceTable,
                        _targetTable,
                        _searchCondition,
                        null,
                        false,
                        null,
                        getContextManager()
                );
    }

    ////////////////////////////////////////////////////////////////////////////////////
    //
    // bind() BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    @Override
    public String statementToString() {
        return "MERGE";
    }

    @Override
    public void bindStatement() throws StandardException
    {
        DataDictionary dd = getDataDictionary();

        //
        // Bind the left join. This binds _targetTable and _sourceTable.
        //
        bind( dd );

        bindSearchCondition();

        if ( !targetIsBaseTable() )
        {
            throw new RuntimeException("SQLState.LANG_TARGET_NOT_BASE_TABLE");
        }

        if ( !sourceIsBase_View_or_VTI() )
        {
            throw new RuntimeException("SQLState.LANG_SOURCE_NOT_BASE_VIEW_OR_VTI");
        }

        // source and target may not have the same correlation names
        if ( getExposedName( _targetTable ).equals( getExposedName( _sourceTable ) ) )
        {
            throw new RuntimeException("SQLState.LANG_SAME_EXPOSED_NAME");
        }

        for ( MatchingClauseNode mcn : _matchingClauses )
        {
            mcn.bind( (JoinNode) resultSet, _targetTable );
        }

        throw new RuntimeException("SQLState.NOT_IMPLEMENTED MERGE");
    }

    /** Get the exposed name of a FromTable */
    private String  getExposedName( FromTable ft ) throws StandardException
    {
        return ft.getTableName().getTableName();
    }

    /**  Bind the search condition, the ON clause of the left join */
    private void    bindSearchCondition()   throws StandardException
    {
        FromList    fromList = new FromList
                ( getNodeFactory().doJoinOrderOptimization(), getContextManager() );

        resultSet.bindResultColumns( fromList );
    }

    /** Return true if the target table is a base table */
    private boolean targetIsBaseTable() throws StandardException
    {
        if ( !( _targetTable instanceof FromBaseTable) ) { return false; }

        FromBaseTable   fbt = (FromBaseTable) _targetTable;
        TableDescriptor desc = fbt.getTableDescriptor();
        if ( desc == null ) { return false; }

        return ( desc.getTableType() == TableDescriptor.BASE_TABLE_TYPE );
    }

    /** Return true if the source table is a base table, view, or table function */
    private boolean sourceIsBase_View_or_VTI() throws StandardException
    {
        if ( _sourceTable instanceof FromVTI ) { return true; }
        if ( !( _sourceTable instanceof FromBaseTable) ) { return false; }

        FromBaseTable   fbt = (FromBaseTable) _sourceTable;
        TableDescriptor desc = fbt.getTableDescriptor();
        if ( desc == null ) { return false; }

        switch( desc.getTableType() )
        {
            case TableDescriptor.BASE_TABLE_TYPE:
            case TableDescriptor.VIEW_TYPE:
                return true;

            default:
                return false;
        }
    }

}
