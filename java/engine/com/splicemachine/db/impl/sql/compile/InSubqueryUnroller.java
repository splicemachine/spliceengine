package com.splicemachine.db.impl.sql.compile;


import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.util.JBitSet;
import com.splicemachine.db.impl.ast.AbstractSpliceVisitor;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;

/**
 *  This Class is added by SpliceMachine. It unrolls In Subquery to a join if it
 *  does not contain any column reference from the outside. In other words, any
 *  In Subquery that can be executed on its on will be flattened.
 *  Example:
 *      Select c1 from t1 where c2 in (select d1 from t2)
 *  will be flattened to:
 *      Select c1, (select d1 from t2) foo where c1 = foo.d1
 *
 */
public class InSubqueryUnroller extends AbstractSpliceVisitor implements Visitor {
    int count = 0;
    @Override
    public Visitable visit(Visitable node) throws StandardException{
        if(node instanceof SelectNode && ((SelectNode) node).getWhereSubquerys() != null){
            ArrayList<SubqueryNode> nodesToSwitch = new ArrayList<>();
            SelectNode select = (SelectNode)node;

            HashSet<String> rcNames = new HashSet<>();
            for(ResultColumn rc : select.resultColumns){
                rcNames.add(rc.name);
            }

            /*
                Check for every subquery and find In Subquery with no outside
                ColumnReference, is invariant and does not contain a not node.
                If the resultset of any subquery is not a select node, we do not
                unroll this query.
             */
            for(SubqueryNode sub : select.getWhereSubquerys()){
                if(!(sub.resultSet instanceof SelectNode)){
                    return node;
                }
                HasOuterCRVisitor visitor = new HasOuterCRVisitor(select.getNestingLevel() + 1);
                sub.resultSet.accept(visitor);
                if(sub.subqueryType == SubqueryNode.IN_SUBQUERY && !visitor.hasCorrelatedCRs() && sub.isInvariant()
                        && !checkNotIn(select.whereClause, sub) && !checkNameCollision(rcNames, sub)){
                    nodesToSwitch.add(sub);
                }
            }
            /*
                Update table numbers of ColumnReferences as we add more FromTables.
             */
            int diff = nodesToSwitch.size();
            for(SubqueryNode sub : select.getWhereSubquerys()){
                AggregateSubqueryFlatteningVisitor.updateTableNumbers((SelectNode)sub.resultSet, diff, ((SelectNode) sub.resultSet).getNestingLevel());
            }

            CompilerContext cpt = select.getCompilerContext();
            cpt.setNumTables(cpt.getNumTables() + nodesToSwitch.size());
            /*
                Process every qualified subquery node into FromSubquery and remove it
                from the SubqueryList of its parent.
             */
            for(SubqueryNode sub : nodesToSwitch){
                SelectNode subquerySelectNode = (SelectNode)sub.resultSet;
                subquerySelectNode.isFlattenedInSubquery = true;
                select.getWhereSubquerys().removeElement(sub);
                ResultColumnList newRcl = subquerySelectNode.resultColumns.copyListAndObjects();
                newRcl.genVirtualColumnNodes(subquerySelectNode, subquerySelectNode.resultColumns);

                FromSubquery fromSubquery = (FromSubquery) subquerySelectNode.getNodeFactory().getNode(C_NodeTypes.FROM_SUBQUERY,
                        subquerySelectNode,
                        sub.getOrderByList(),
                        sub.getOffset(),
                        sub.getFetchFirst(),
                        sub.hasJDBClimitClause(),
                        getSubqueryAlias(),
                        newRcl,
                        null,
                        select.getContextManager());
                FromList fl = select.fromList;
                int ind = fl.size() - 1;
                while (!(fl.elementAt(ind) instanceof FromTable)) {
                    ind--;
                }
                FromTable ft = (FromTable) fl.elementAt(ind);
                fromSubquery.tableNumber = ft.tableNumber + 1;
                fromSubquery.level = ft.getLevel();
                fl.addFromTable(fromSubquery);
                ValueNode newTopWhereClause = switchPredReference(select.whereClause, fromSubquery, select.getNestingLevel());
                select.originalWhereClause = newTopWhereClause;
                select.whereClause = newTopWhereClause;
            }

        }
        return node;
    }
    @Override
    public boolean stopTraversal() {
        return false;
    }

    @Override
    public boolean visitChildrenFirst(Visitable node) {
        return false;
    }

    @Override
    public boolean skipChildren(Visitable node) {
        return false;
    }

    private String getSubqueryAlias(){
        String ret = "FlattenedInSubquery" + count;
        count++;
        return ret;
    }
    /*
        Transform the where clause of the parent select node. We remove the in subquery node
        from the where clause and construct a BinaryRelationalOperator node to replace it.
     */
    public static ValueNode switchPredReference(ValueNode node,
                                                FromSubquery fsq, int level) throws StandardException {
        if (node instanceof BinaryOperatorNode) {
            BinaryOperatorNode root = (BinaryOperatorNode) node;
            ValueNode left = root.getLeftOperand();
            ValueNode right = root.getRightOperand();
            ColumnReference cf = (ColumnReference) fsq.getNodeFactory().getNode(C_NodeTypes.COLUMN_REFERENCE,
                    fsq.resultColumns.elementAt(0).name,
                    fsq.getTableName(),
                    fsq.getContextManager());
            cf.setSource(fsq.resultColumns.elementAt(0));
            cf.setTableNumber(fsq.tableNumber);
            cf.setColumnNumber(fsq.resultColumns.elementAt(0).getVirtualColumnId());
            cf.setNestingLevel(level);
            cf.setSourceLevel(level);
            if (left instanceof SubqueryNode && ((SubqueryNode) left).getResultSet() == fsq.getSubquery()) {
                ValueNode ncf = ((SubqueryNode) left).leftOperand;
                BinaryRelationalOperatorNode bro = (BinaryRelationalOperatorNode)root.getNodeFactory().getNode(
                        C_NodeTypes.BINARY_EQUALS_OPERATOR_NODE,
                        ncf,
                        cf,
                        root.getContextManager());
                DataTypeDescriptor dtd = DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN);
                bro.setType(dtd);
                root.setLeftOperand(bro);
                return root;
            } else if (right instanceof SubqueryNode && ((SubqueryNode) right).getResultSet() == fsq.getSubquery()) {
                ValueNode ncf = ((SubqueryNode) right).leftOperand;
                BinaryRelationalOperatorNode bro = (BinaryRelationalOperatorNode)root.getNodeFactory().getNode(
                        C_NodeTypes.BINARY_EQUALS_OPERATOR_NODE,
                        ncf,
                        cf,
                        root.getContextManager());
                DataTypeDescriptor dtd = DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN);
                bro.setType(dtd);
                root.setRightOperand(bro);
                return root;
            } else {
                left = switchPredReference(left, fsq, level);
                right = switchPredReference(right, fsq, level);
                root.setLeftOperand(left);
                root.setRightOperand(right);
                return root;
            }
        } else if (node instanceof ColumnReference) {
            return node;
        }
        else if(node instanceof SubqueryNode && ((SubqueryNode) node).getResultSet() == fsq.getSubquery()){
            ColumnReference cf = (ColumnReference) fsq.getNodeFactory().getNode(C_NodeTypes.COLUMN_REFERENCE,
                    fsq.resultColumns.elementAt(0).name,
                    fsq.getTableName(),
                    fsq.getContextManager());
            cf.setSource(fsq.resultColumns.elementAt(0));
            cf.setTableNumber(fsq.tableNumber);
            cf.setColumnNumber(fsq.resultColumns.elementAt(0).getVirtualColumnId());
            cf.setNestingLevel(level);
            cf.setSourceLevel(level);
            ValueNode ncf = ((SubqueryNode) node).leftOperand;
            BinaryRelationalOperatorNode bro = (BinaryRelationalOperatorNode)node.getNodeFactory().getNode(
                    C_NodeTypes.BINARY_EQUALS_OPERATOR_NODE,
                    ncf,
                    cf,
                    node.getContextManager());
            DataTypeDescriptor dtd = DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN);
            bro.setType(dtd);
            return bro;
        }
        return node;
    }
    /*
        check recursively if the where clause contains a not node. If it does,
        we do not unroll this subquery.
     */

    public static boolean checkNotIn(ValueNode whereClause, SubqueryNode sub){
        if(whereClause instanceof BinaryOperatorNode){
            return checkNotIn(((BinaryOperatorNode) whereClause).leftOperand, sub) ||
                    checkNotIn(((BinaryOperatorNode) whereClause).rightOperand, sub);
        }
        else if(whereClause instanceof NotNode && ((NotNode) whereClause).operand == sub){
            return true;
        }
        return false;
    }

    /*
        check for column name collision between the subquery's resultColumns and
        its parent select node's resultColumns.
     */
    public static boolean checkNameCollision(HashSet<String> parent, SubqueryNode sub){
        if(!(sub.resultSet instanceof SelectNode)){
            return true;
        }
        for(ResultColumn rc : sub.resultSet.resultColumns){
            if(parent.contains(rc.name)){
                return true;
            }
        }
        return false;
    }


}
