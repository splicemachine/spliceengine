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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */
package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.sql.dictionary.SequenceDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;

import java.util.Collections;
import java.util.List;
import java.sql.Types;

/**
 * A class that represents a value obtained from a Sequence using 'NEXT VALUE'
 */
public class NextSequenceNode extends ValueNode {

    private TableName sequenceName;
    private SequenceDescriptor sequenceDescriptor;

    /**
     * Initializer for a NextSequenceNode
     *
     * @param sequenceName The name of the sequence being called
     * @throws com.splicemachine.db.iapi.error.StandardException
     *          Thrown on error
     */
    public void init(Object sequenceName) throws StandardException {
        this.sequenceName = (TableName) sequenceName;
    }

    /**
     * Bind this expression.  This means binding the sub-expressions,
     * as well as figuring out what the return type is for this expression.
     *
     * @param fromList        The FROM list for the query this
     *                        expression is in, for binding columns.
     * @param subqueryList    The subquery list being built as we find SubqueryNodes
     * @param aggregateVector The aggregate vector being built as we find AggregateNodes
     * @return The new top of the expression tree.
     * @throws StandardException Thrown on error
     */
    @Override
    public ValueNode bindExpression(FromList fromList,
                                    SubqueryList subqueryList,
                                    List<AggregateNode> aggregateVector,
                                    boolean forQueryRewrite) throws StandardException {
        //
        // Higher level bind() logic may try to redundantly re-bind this node. Unfortunately,
        // that causes us to think that the sequence is being referenced more than once
        // in the same statement. If the sequence generator is already filled in, then
        // this node has already been bound and we can exit quickly. See DERBY-4803.
        //
        if ( sequenceDescriptor != null ) { return this; }
        
        CompilerContext cc = getCompilerContext();
        
        if ( (cc.getReliability() & CompilerContext.NEXT_VALUE_FOR_ILLEGAL) != 0 )
        {
            throw StandardException.newException( SQLState.LANG_NEXT_VALUE_FOR_ILLEGAL );
        }

        // lookup sequence object in the data dictionary
        SchemaDescriptor sd = getSchemaDescriptor(sequenceName.getSchemaName());
        sequenceDescriptor = getDataDictionary().getSequenceDescriptor(sd, sequenceName.getTableName());

        if ( sequenceDescriptor == null )
        {
                throw StandardException.newException(SQLState.LANG_OBJECT_NOT_FOUND, "SEQUENCE", sequenceName.getFullTableName());
        }

        // set the datatype of the value node
        this.setType(sequenceDescriptor.getDataType());

        //
        // The statement is only allowed to refer to a given sequence once.
        // See DERBY-4513.
        //
        if ( cc.isReferenced( sequenceDescriptor ) )
        {
            throw StandardException.newException
                ( SQLState.LANG_SEQUENCE_REFERENCED_TWICE, sequenceName.getFullTableName() );
        }
        cc.addReferencedSequence( sequenceDescriptor );

        ValueNode returnNode = this;

        // set up dependency on sequence and compile a check for USAGE
        // priv if needed
        getCompilerContext().createDependency( sequenceDescriptor );

        if ( isPrivilegeCollectionRequired() )
        {
            getCompilerContext().addRequiredUsagePriv( sequenceDescriptor );
        }

        return returnNode;
    }


    public void generateExpression
        (
         ExpressionClassBuilder acb,
         MethodBuilder mb
         )
        throws StandardException
    {
        String sequenceUUIDstring = sequenceDescriptor.getUUID().toString();
        int dataTypeFormatID = sequenceDescriptor.getDataType().getNull().getTypeFormatId();
        
		mb.pushThis();
		mb.push( sequenceUUIDstring );
		mb.push( dataTypeFormatID );
		mb.callMethod
            (
             VMOpcode.INVOKEVIRTUAL,
             ClassName.BaseActivation,
             "getCurrentValueAndAdvance",
             ClassName.NumberDataValue,
             2
             );
    }

    /**
     * Dummy implementation to return a constant. Will be replaced with actual NEXT VALUE logic.
     *
     * @param ecb The ExpressionClassBuilder for the class being built
     * @param mb The method the expression will go into
     * @throws StandardException on error
     */
    public void generateConstant
            (
                    ExpressionClassBuilder ecb,
                    MethodBuilder mb
            ) throws StandardException {
        switch (getTypeServices().getJDBCTypeId()) {
            case Types.INTEGER:
                mb.push(1);
                break;
            default:
                if (SanityManager.DEBUG) {
                    SanityManager.THROWASSERT(
                            "Unexpected dataType = " + getTypeServices().getJDBCTypeId());
                }
        }

    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */

    public String toString() {
        if (SanityManager.DEBUG) {
            return super.toString();
        } else {
            return "";
        }
    }

    protected boolean isEquivalent(ValueNode other) throws StandardException {
        return this == other;
    }

	public List getChildren() {
		return Collections.EMPTY_LIST;
	}
}
