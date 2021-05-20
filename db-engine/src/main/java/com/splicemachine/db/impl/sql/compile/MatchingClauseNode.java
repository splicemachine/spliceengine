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
