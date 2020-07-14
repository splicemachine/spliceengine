/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.db.iapi.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.context.Context;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.loader.ClassInfo;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionFactory;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.impl.sql.compile.*;
import com.splicemachine.db.impl.sql.conn.GenericLanguageConnectionContext;
import com.splicemachine.db.impl.sql.conn.GenericLanguageConnectionFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.Assert;
import org.junit.Test;

import static com.splicemachine.db.iapi.types.TypeId.REF_NAME;
import static com.splicemachine.db.iapi.types.TypeId.getBuiltInTypeId;

public class ProjectRestrictNodeTest {

    static class MockGenericLanguageConnectionFactory extends GenericLanguageConnectionFactory {
        public NodeFactory getNodeFactory()
        {
            return new MockNodeFactory();
        }
    }

    @SuppressFBWarnings(value="HE_INHERITS_EQUALS_USE_HASHCODE", justification="test class, for fixing business logic review DB-9277")
    static class MockCurrentRowLocationNode extends CurrentRowLocationNode {
        @Override
        public CurrentRowLocationNode getClone() throws StandardException {
            return (CurrentRowLocationNode) getNodeFactory().getNode(C_NodeTypes.CURRENT_ROW_LOCATION_NODE, getContextManager());
        }
    }

    static class MockGenericLanguageConnectionContext extends GenericLanguageConnectionContext {

        public MockGenericLanguageConnectionContext(ContextManager cm) throws StandardException {
            super(cm, null, null, new GenericLanguageConnectionFactory(), null, null, null, 0, null, null, null, DataSetProcessorType.DEFAULT_CONTROL, true, 1, null, null, null );
        }

        public LanguageConnectionFactory getLanguageConnectionFactory(){
            return new MockGenericLanguageConnectionFactory();
        }
    }

    static class MockNodeFactory extends NodeFactory {
        private final ClassInfo[] nodeCi = new ClassInfo[C_NodeTypes.FINAL_VALUE+1];
        public MockNodeFactory() {}

        public Boolean doJoinOrderOptimization() {
            return true;
        }
        public Node getNode(int nodeType, ContextManager cm) throws StandardException {
            ClassInfo ci = nodeCi[nodeType];
            Class nodeClass;
            if (ci == null) {
                String nodeName = nodeName(nodeType);
                try {
                    nodeClass = Class.forName(nodeName);
                } catch (ClassNotFoundException e) {
                    throw new StandardException();
                }
                ci = new ClassInfo(nodeClass);
                nodeCi[nodeType] = ci;
            }
            QueryTreeNode retval;
            try {
                retval = (QueryTreeNode) ci.getNewInstance();
            } catch (Exception iae) {
                throw new StandardException();
            }
            retval.setContextManager(cm);
            retval.setNodeType(nodeType);
            return retval;
        }

        protected String nodeName(int nodeType) throws StandardException
        {
            switch (nodeType)
            {
                case C_NodeTypes.PROJECT_RESTRICT_NODE:
                    return C_NodeNames.PROJECT_RESTRICT_NODE_NAME;
                case C_NodeTypes.RESULT_COLUMN_LIST:
                    return C_NodeNames.RESULT_COLUMN_LIST_NAME;
                case C_NodeTypes.VIRTUAL_COLUMN_NODE:
                    return C_NodeNames.VIRTUAL_COLUMN_NODE_NAME;
                case C_NodeTypes.RESULT_COLUMN:
                    return C_NodeNames.RESULT_COLUMN_NAME;
                case C_NodeTypes.CURRENT_ROW_LOCATION_NODE:
                    return C_NodeNames.CURRENT_ROW_LOCATION_NODE_NAME;
                default:
                    throw StandardException.newException(SQLState.NOT_IMPLEMENTED, nodeType);
            }
        }
    }

    static class MockContextManager extends ContextManager {
        public MockContextManager() {
            super(null, null);
        }

        public Context getContext(String contextId) {
            if(contextId.equals(LanguageConnectionContext.CONTEXT_ID)) {
                try {
                    return new MockGenericLanguageConnectionContext(this);
                } catch (StandardException e) {
                    return null;
                }
            }
            return null;
        }
    }

    static class MockFromBaseTable extends FromBaseTable {

        private final ResultColumn resultColumn;

        MockFromBaseTable(ResultColumn resultColumn) {
            this.resultColumn = resultColumn;
        }

        @Override
        public ResultColumn getRowIdColumn(){
            return resultColumn;
        }

        @Override
        public ResultSetNode changeAccessPath() {
            return new MockFromBaseTable(resultColumn);
        }
    }

    static class MockResultColumn extends ResultColumn {
        public void setType(DataTypeDescriptor descriptor) {
        }
    }

    static class MockPRN extends ProjectRestrictNode {
        public void processRowIdReferenceInFromBaseTableChild() throws StandardException {
            super.processRowIdReferenceInFromBaseTableChild();
        }
    }

    ResultColumn constructRowIdCol() throws StandardException {
        ResultColumn result = new MockResultColumn();
        result.setName("ROWID");
        result.setContextManager(new MockContextManager());
        result.setType(new DataTypeDescriptor(getBuiltInTypeId(REF_NAME), true));
        CurrentRowLocationNode currentRowLocationNode = new MockCurrentRowLocationNode();
        currentRowLocationNode.setContextManager(new MockContextManager());
        result.setExpression(currentRowLocationNode);
        return result;
    }

    ResultColumn constructNormalCol() throws StandardException {
        ResultColumn result = new MockResultColumn();
        result.setContextManager(new MockContextManager());
        result.setName("col1");
        result.setType(DataTypeDescriptor.INTEGER);
        return result;
    }

    ColumnReference constructColumnReference(ResultColumn resultColumn) {
        ColumnReference columnReference = new ColumnReference();
        columnReference.setContextManager(new MockContextManager());
        columnReference.setSource(resultColumn);
        return columnReference;
    }

    Predicate constructPredicate(ResultColumn resultColumn) {
        Predicate p = new Predicate();
        AndNode andNode = new AndNode();
        BinaryRelationalOperatorNode left = new BinaryRelationalOperatorNode();
        left.setLeftOperand(constructColumnReference(resultColumn));
        left.setRightOperand(new CharConstantNode());
        BooleanConstantNode right = new BooleanConstantNode();
        andNode.setLeftOperand(left);
        andNode.setRightOperand(right);
        p.setAndNode(andNode);
        return p;
    }

    MockPRN constructPRN(ResultColumn resultColumn) throws Exception {
        MockPRN prn = new MockPRN();
        prn.setContextManager(new MockContextManager());
        ResultColumnList resultColumns = new ResultColumnList();
        resultColumns.setContextManager(new MockContextManager());
        resultColumns.addResultColumn(constructNormalCol());
        resultColumns.addResultColumn(resultColumn);
        prn.init(new MockFromBaseTable(resultColumn), resultColumns, null, new PredicateList(), null, null, null);
        prn.restrictionList.addPredicate(constructPredicate(resultColumn));
        return prn;
    }

    @Test
    public void rowIdWithRestrictionListCausesGenerationOfAnotherPRNode() throws Exception {
        MockPRN prn = constructPRN(constructRowIdCol());
        prn.processRowIdReferenceInFromBaseTableChild();
        ResultSetNode resultSetNode = prn.getChildResult();
        Assert.assertTrue(resultSetNode instanceof ProjectRestrictNode);
        ProjectRestrictNode childPRN = (ProjectRestrictNode)resultSetNode;
        Assert.assertEquals(2, childPRN.getResultColumns().size());
        ResultColumn resultColumn1 = childPRN.getResultColumns().getResultColumn(1);
        Assert.assertEquals("col1", resultColumn1.getName());
        ResultColumn resultColumn2 = childPRN.getResultColumns().getResultColumn(2);
        Assert.assertEquals("ROWID", resultColumn2.getName());
        Assert.assertEquals(2, prn.getResultColumns().size());
        Assert.assertTrue(prn.getResultColumns().getResultColumn(1).getExpression() instanceof VirtualColumnNode);
        Assert.assertTrue(prn.getResultColumns().getResultColumn(2).getExpression() instanceof VirtualColumnNode);
        Assert.assertNull(childPRN.restrictionList);
    }
}
