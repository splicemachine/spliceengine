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
import com.splicemachine.db.iapi.sql.compile.Visitor;
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

    public MergeNode(FromTable targetTable, FromTable sourceTable, ValueNode searchCondition,
                     QueryTreeNodeVector<MatchingClauseNode> matchingClauses, ContextManager cm)
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

    // todo: this is dummy code
    @Override
    public ConstantAction makeConstantAction() throws StandardException
    {
        return getGenericConstantActionFactory().
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

}
