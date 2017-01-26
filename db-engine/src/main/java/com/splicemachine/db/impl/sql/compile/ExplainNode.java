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
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.impl.sql.GenericColumnDescriptor;
import com.splicemachine.db.iapi.types.TypeId;

import java.util.Collection;

/**
 * @author Jun Yuan
 * Date: 6/9/14
 */
public class ExplainNode extends DMLStatementNode {

    StatementNode node;

    int activationKind() { return StatementNode.NEED_NOTHING_ACTIVATION; }

    public String statementToString() { return "Explain"; }

    public void init(Object statementNode) { node = (StatementNode)statementNode; }

    /**
     * Used by splice. Provides direct access to the node underlying the explain node.
     * @return the root of the actual execution plan.
     */
    @SuppressWarnings("UnusedDeclaration")
    public StatementNode getPlanRoot(){
        return node;
    }

    @Override
    public void optimizeStatement() throws StandardException {
        node.optimizeStatement();
    }

    @Override
    public void bindStatement() throws StandardException {
        node.bindStatement();
    }

    @Override
    public void generate(ActivationClassBuilder acb, MethodBuilder mb) throws StandardException {
        /*
         * Explain Operations should always use the control side (since they don't actually move any data).
         * If you don't set this here, and if the underlying tablescan is believed to cost more than a
         * certain fixed number, then we will perform the Explain in Spark, which will be brutal and useless.
         * This forces us to use control-side execution
         */
        getCompilerContext().setDataSetProcessorType(CompilerContext.DataSetProcessorType.FORCED_CONTROL);
        acb.pushGetResultSetFactoryExpression(mb);
        // parameter
        node.generate(acb, mb);
        acb.pushThisAsActivation(mb);
        int resultSetNumber = getCompilerContext().getNextResultSetNumber();
        mb.push(resultSetNumber);
        mb.callMethod(VMOpcode.INVOKEINTERFACE,null, "getExplainResultSet", ClassName.NoPutResultSet, 3);
    }

    @Override
    public ResultDescription makeResultDescription() {
        DataTypeDescriptor dtd = new DataTypeDescriptor(TypeId.getBuiltInTypeId(TypeId.VARCHAR_NAME), true);
        ResultColumnDescriptor[] colDescs = new GenericColumnDescriptor[1];
        colDescs[0] = new GenericColumnDescriptor("Plan", dtd);
        String statementType = statementToString();

        return getExecutionFactory().getResultDescription(colDescs, statementType );
    }

    @Override
    public void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);

        if ( node!= null) {
            node = (StatementNode)node.accept(v, this);
        }
    }

    @Override
    public ConstantAction makeConstantAction() throws StandardException {
        return	node.makeConstantAction();
    }

    @Override
    public void buildTree(Collection<QueryTreeNode> tree, int depth) throws StandardException {
        if ( node!= null)
            node.buildTree(tree,depth);
    }
}
