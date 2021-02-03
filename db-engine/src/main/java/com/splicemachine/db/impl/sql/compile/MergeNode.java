/*

   Derby - Class org.apache.derby.impl.sql.compile.MergeNode

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
import com.splicemachine.db.iapi.sql.compile.OptimizerFactory;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;

/**
 * This class represents a MERGE INTO statement.
 */
public class MergeNode extends DDLStatementNode
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

    public  MergeNode
            (
                    FromTable          targetTable,
                    FromTable          sourceTable,
                    ValueNode          searchCondition,
                    QueryTreeNodeVector<MatchingClauseNode> matchingClauses,
                    ContextManager     cm
            )
            throws StandardException
    {
        super(cm);

        if ( !( targetTable instanceof FromBaseTable) ) { notBaseTable(); }
        else { _targetTable = (FromBaseTable) targetTable; }

        _sourceTable = sourceTable;
        _searchCondition = searchCondition;
        _matchingClauses = matchingClauses;
    }

    /** Throw a "not base table" exception */
    private void    notBaseTable()  throws StandardException
    {
        throw new RuntimeException("not FromBaseTable table");
    }

    /**
     * Create the Constant information that will drive the guts of Execution.
     *
     * @exception StandardException Standard error policy.
     */
    @Override
    public ConstantAction makeConstantAction() throws StandardException
    {
        return getGenericConstantActionFactory().
                // getSetSchemaConstantAction("SPLICE", StatementType.SET_SCHEMA_USER);
                getSetSchemaConstantAction("SPLICE", StatementType.SET_SCHEMA_USER);
    }

    @Override
    public void acceptChildren(Visitor v)
            throws StandardException
    {
        if ( _leftJoinCursor != null )
        {
            _leftJoinCursor.acceptChildren( v );
        }
        else
        {
            super.acceptChildren( v );

            _targetTable.accept( v );
            _sourceTable.accept( v );
            _searchCondition.accept( v );
        }

        for ( MatchingClauseNode mcn : _matchingClauses )
        {
            mcn.accept( v );
        }
    }


    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return  This object as a String
     */

    @Override
    public String toString()
    {
        return "MERGE INTO toString";
    } // end of toString

    @Override
    public String statementToString()
    {
        return "MERGE INTO";
    }

    //@Override
    public void bindStatement2() throws StandardException
    {
        DataDictionary dd = getDataDictionary();

        // source table must be a vti or base table
        if (
                !(_sourceTable instanceof FromVTI) &&
                        !(_sourceTable instanceof FromBaseTable)
        )
        {

            throw new RuntimeException("VTI or Table");
            //throw StandardException.newException( SQLState.LANG_SOURCE_NOT_BASE_OR_VTI );
        }

        // source and target may not have the same correlation names
//        if ( getExposedName( _targetTable ).equals( getExposedName( _sourceTable ) ) )
//        {
//            throw StandardException.newException( SQLState.LANG_SAME_EXPOSED_NAME );
//        }

        // don't allow derived column lists right now
//        forbidDerivedColumnLists();

        // synonyms not allowed
//        forbidSynonyms();

        //
        // Don't add any privileges until we bind the matching clauses.
        //
//        IgnoreFilter    ignorePermissions = new IgnoreFilter();
//        getCompilerContext().addPrivilegeFilter( ignorePermissions );

        FromList    dfl = new FromList( getContextManager() );
        FromTable   dflSource = cloneFromTable( _sourceTable );
        FromBaseTable   dflTarget = (FromBaseTable) cloneFromTable( _targetTable );
        dfl.addFromTable( dflSource );
        dfl.addFromTable( dflTarget );
        // todo: doJoinOrderOptimization
        dfl.bindTables( dd, new FromList( /*getOptimizerFactory().doJoinOrderOptimization(),*/ getContextManager() ) );

        // target table must be a base table
        if ( !targetIsBaseTable( dflTarget ) ) { notBaseTable(); }

        // ready to add permissions
        // todo
        //getCompilerContext().removePrivilegeFilter( ignorePermissions );

        for ( MatchingClauseNode mcn : _matchingClauses )
        {
            FromList    dummyFromList = cloneFromList( dd, dflTarget );
            FromBaseTable   dummyTargetTable = (FromBaseTable) dummyFromList.elementAt( TARGET_TABLE_INDEX );
            // todo
            //mcn.bind( dd, this, dummyFromList, dummyTargetTable );

            // window function not allowed
            SelectNode.checkNoWindowFunctions(mcn, "matching clause");

            // aggregates not allowed
            // todo
            //checkNoAggregates(mcn);
        }

        // todo
        //bindLeftJoin( dd );
    }


    /** Create a FromList for binding a WHEN [ NOT ] MATCHED clause */
    private FromList    cloneFromList( DataDictionary dd, FromBaseTable targetTable )
            throws StandardException
    {
        FromList    dummyFromList = new FromList( getContextManager() );
        FromBaseTable   dummyTargetTable = new FromBaseTable
                (
                        targetTable.getTableNameField(),
                        targetTable.correlationName,
                        null,
                        null,
                        getContextManager()
                );
        FromTable       dummySourceTable = cloneFromTable( _sourceTable );

        // todo
//        dummyTargetTable.setMergeTableID( ColumnReference.MERGE_TARGET );
//        dummySourceTable.setMergeTableID ( ColumnReference.MERGE_SOURCE );

        dummyFromList.addFromTable( dummySourceTable );
        dummyFromList.addFromTable( dummyTargetTable );

        //
        // Don't add any privileges while binding the tables.
        //
//        IgnoreFilter    ignorePermissions = new IgnoreFilter();
//        getCompilerContext().addPrivilegeFilter( ignorePermissions );

        // todo
        dummyFromList.bindTables( dd, new FromList( /*getOptimizerFactory().doJoinOrderOptimization(),*/ getContextManager() ) );

        // ready to add permissions
        //getCompilerContext().removePrivilegeFilter( ignorePermissions );

        return dummyFromList;
    }
    /** Clone a FromTable to avoid binding the original */
    private FromTable cloneFromTable( FromTable fromTable ) throws StandardException
    {
        if ( fromTable instanceof FromVTI )
        {
            FromVTI source = (FromVTI) fromTable;

            return new FromVTI
                    (
                            source.methodCall,
                            source.correlationName,
                            source.getResultColumns(),
                            null,
                            source.exposedName,
                            null, // type descriptor
                            getContextManager()
                    );
        }
        else if ( fromTable instanceof FromBaseTable )
        {
            FromBaseTable   source = (FromBaseTable) fromTable;
            return new FromBaseTable
                    (
                            source.tableName,
                            source.correlationName,
                            null,
                            null,
                            getContextManager()
                    );
        }
        else
        {
            throw new RuntimeException("LANG_SOURCE_NOT_BASE_OR_VTI");
        }
    }

    /** Return true if the target table is a base table */
    private boolean targetIsBaseTable( FromBaseTable targetTable ) throws StandardException
    {
        FromBaseTable   fbt = targetTable;
        TableDescriptor desc = fbt.getTableDescriptor();
        if ( desc == null ) { return false; }

        switch( desc.getTableType() )
        {
            case TableDescriptor.BASE_TABLE_TYPE:
            case TableDescriptor.LOCAL_TEMPORARY_TABLE_TYPE: // todo: check if this is really LOCAL, not GLOBAL
                return true;

            default:
                return false;
        }
    }
}
