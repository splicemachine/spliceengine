/*

   Derby - Class org.apache.derby.impl.sql.compile.MatchingClauseNode

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

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextManager;

public class MatchingClauseNode extends QueryTreeNode {

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static final String CURRENT_OF_NODE_NAME = "$MERGE_CURRENT";

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    //
    // Filled in by the constructor.
    //
    private ValueNode           _matchingRefinement;
    private ResultColumnList    _updateColumns;
    private ResultColumnList    _insertColumns;
    private ResultColumnList    _insertValues;

    //
    // Filled in at bind() time.
    //

    // the INSERT/UPDATE/DELETE statement of this WHEN [ NOT ] MATCHED clause
    private DMLModStatementNode _dml;

    // the columns in the temporary conglomerate which drives the INSERT/UPDATE/DELETE
    private ResultColumnList        _thenColumns;

    //
    // Filled in at generate() time.
    //
    private int     _clauseNumber;
    private String  _actionMethodName;
    private String  _resultSetFieldName;
    private String  _rowMakingMethodName;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTORS/FACTORY METHODS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Constructor called by factory methods.
     */
    private MatchingClauseNode (ValueNode matchingRefinement, ResultColumnList updateColumns,
                                ResultColumnList insertColumns, ResultColumnList insertValues, ContextManager cm)
            throws StandardException
    {
        super( cm );

        _matchingRefinement = matchingRefinement;
        _updateColumns = updateColumns;
        _insertColumns = insertColumns;
        _insertValues = insertValues;
    }

    /** Make a WHEN MATCHED ... THEN UPDATE clause */
    static MatchingClauseNode makeUpdateClause (ValueNode matchingRefinement, ResultColumnList updateColumns,
                                                ContextManager cm)
            throws StandardException
    {
        return new MatchingClauseNode( matchingRefinement, updateColumns, null, null, cm );
    }

    /** Make a WHEN MATCHED ... THEN DELETE clause */
    static MatchingClauseNode makeDeleteClause(ValueNode matchingRefinement, ContextManager cm)
            throws StandardException
    {
        return new MatchingClauseNode( matchingRefinement, null, null, null, cm );
    }

    /** Make a WHEN NOT MATCHED ... THEN INSERT clause */
    static MatchingClauseNode makeInsertClause(ValueNode matchingRefinement, ResultColumnList insertColumns,
                                               ResultColumnList insertValues, ContextManager cm)
            throws StandardException
    {
        return new MatchingClauseNode( matchingRefinement, null, insertColumns, insertValues, cm );
    }
}
