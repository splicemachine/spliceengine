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
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.io.FormatableArrayHolder;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.compile.DataSetProcessorType;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.impl.sql.GenericColumnDescriptor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

/**
 * @author Jun Yuan
 * Date: 6/9/14
 */
public class ExplainNode extends DMLStatementNode {

    StatementNode node;
    private SparkExplainKind sparkExplainKind;
    private boolean showNoStatsObjects;

    private final List<SQLVarchar> noStatsTables  = new ArrayList<>();
    private final List<SQLVarchar> noStatsColumns = new ArrayList<>();

    public enum SparkExplainKind {
        NONE("none"),
        EXECUTED("executed"),
        LOGICAL("logical"),
        OPTIMIZED("optimized"),
        ANALYZED("analyzed");

        private final String value;

        SparkExplainKind(String value) {
            this.value = value;
        }

        public final String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return value;
        }
    }

    int activationKind() { return StatementNode.NEED_NOTHING_ACTIVATION; }

    public String statementToString() { return "Explain"; }

    public void init(Object statementNode,
                     Object sparkExplainKind,
                     Object showNoStatsObjects) {
        node = (StatementNode)statementNode;
        this.sparkExplainKind = (SparkExplainKind)sparkExplainKind;
        this.showNoStatsObjects = (Boolean)showNoStatsObjects;
    }

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
        if (sparkExplainKind != SparkExplainKind.NONE) {
            getCompilerContext().setDataSetProcessorType(DataSetProcessorType.FORCED_SPARK);
        }
        node.optimizeStatement();

        // collect tables and columns that are missing statistics only for splice explain
        // showNoStatsObjects == false for all kinds of spark explain
        if (showNoStatsObjects) {
            collectNoStatsTablesAndColumns();
        }
    }

    @Override
    public void bindStatement() throws StandardException {
        node.bindStatement();
    }

    @Override
    public void generate(ActivationClassBuilder acb, MethodBuilder mb) throws StandardException {
        acb.pushGetResultSetFactoryExpression(mb);
        // parameter
        mb.setSparkExplain(sparkExplainKind != SparkExplainKind.NONE);
        node.generate(acb, mb);
        acb.pushThisAsActivation(mb);
        int resultSetNumber = getCompilerContext().getNextResultSetNumber();
        mb.push(resultSetNumber);
        mb.push(sparkExplainKind.toString());

        int noStatsTablesRef = acb.addItem(new FormatableArrayHolder(noStatsTables.toArray()));
        mb.push(noStatsTablesRef);

        int noStatsColumnsRef = acb.addItem(new FormatableArrayHolder(noStatsColumns.toArray()));
        mb.push(noStatsColumnsRef);

        mb.callMethod(VMOpcode.INVOKEINTERFACE,null, "getExplainResultSet", ClassName.NoPutResultSet, 6);
    }

    @Override
    public ResultDescription makeResultDescription() {
        DataTypeDescriptor dtd = new DataTypeDescriptor(TypeId.getBuiltInTypeId(TypeId.VARCHAR_NAME), true);
        ResultColumnDescriptor[] colDescs = new GenericColumnDescriptor[1];
        String headerString = null;
        switch (sparkExplainKind) {
            case EXECUTED:
                headerString = "\nNative Spark Execution Plan";
                break;
            case LOGICAL :
                headerString = "\nNative Spark Logical Plan";
                break;
            case OPTIMIZED :
                headerString = "\nNative Spark Optimized Plan";
                break;
            case ANALYZED :
                headerString = "\nNative Spark Analyzed Plan";
                break;
            default :
                headerString = "Plan";
                break;
        }
        colDescs[0] = new GenericColumnDescriptor(headerString, dtd);
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

    private void collectNoStatsTablesAndColumns() throws StandardException {
        HashSet<String> noStatsColumnSet = new HashSet<>();

        // collect no stats columns used to estimate scan cost
        CollectNodesVisitor cnv = new CollectNodesVisitor(FromBaseTable.class);
        node.accept(cnv);
        List<FromBaseTable> baseTableNodes = cnv.getList();
        for (FromBaseTable t : baseTableNodes) {
            String tableName = t.getExposedName();
            if (!t.useRealTableStats()) {
                noStatsTables.add(new SQLVarchar(tableName));
            } else if (!t.getNoStatsColumnIds().isEmpty()) {
                TableDescriptor td = t.getTableDescriptor();
                for (int columnId : t.getNoStatsColumnIds()) {
                    noStatsColumnSet.add(tableName + "." + td.getColumnDescriptor(columnId).getColumnName());
                }
            }
        }

        // collect no stats columns used to estimate join selectivity
        cnv = new CollectNodesVisitor(BinaryRelationalOperatorNode.class);
        node.accept(cnv);
        List<BinaryRelationalOperatorNode> binaryOpNodes = cnv.getList();
        for (BinaryRelationalOperatorNode bop : binaryOpNodes) {
            noStatsColumnSet.addAll(bop.getNoStatsColumns());
        }

        // collect no stats columns used to estimate grouping cardinality
        cnv = new CollectNodesVisitor(GroupByNode.class);
        node.accept(cnv);
        List<GroupByNode> groupByNodes = cnv.getList();
        for (GroupByNode gbn : groupByNodes) {
            noStatsColumnSet.addAll(gbn.getNoStatsColumns());
        }

        for (String columnName : noStatsColumnSet) {
            noStatsColumns.add(new SQLVarchar(columnName));
        }
    }
}
