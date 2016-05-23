package com.splicemachine.db.impl.ast;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.NodeFactory;
import com.splicemachine.db.iapi.sql.compile.Optimizable;
import com.splicemachine.db.iapi.sql.conn.ConnectionUtil;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.impl.sql.compile.*;
import com.splicemachine.db.impl.sql.compile.Predicate;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.sparkproject.guava.collect.Lists;
import org.sparkproject.guava.collect.Sets;
import org.sparkproject.guava.primitives.Ints;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Find columns used in equijoin predicates and set the {left,right}HashKeys field on the JoinNode.
 * <p/>
 * Dependency: relies on join predicates having been pulled up from leaf nodes and attached
 * directly to join in the JoinConditionVisitor.
 *
 * @author P trolard
 *         Date: 18/10/2013
 */

public class FindHashJoinColumns extends AbstractSpliceVisitor {

    private static Logger LOG = Logger.getLogger(FindHashJoinColumns.class);

    @Override
    public JoinNode visit(JoinNode node) throws StandardException {
        if (!RSUtils.isHashableJoin(((Optimizable) node.getRightResultSet()).getTrulyTheBestAccessPath())) {
            return node;
        }
        Set<Predicate> equiJoinPreds =
                Sets.filter(Sets.newLinkedHashSet(PredicateUtils.PLtoList(node.joinPredicates)),
                        PredicateUtils.isEquiJoinPred);

        Pair<List<Integer>, List<Integer>> indices = findHashIndices(node, equiJoinPreds);
        if (LOG.isDebugEnabled())
            LOG.debug(String.format("Hash key indices found for Join n=%s: %s", node.getResultSetNumber(), indices));
        node.leftHashKeys = Ints.toArray(indices.getLeft());
        node.rightHashKeys = Ints.toArray(indices.getRight());
        return node;
    }

    @Override
    public JoinNode visit(HalfOuterJoinNode node) throws StandardException {
        return visit((JoinNode) node);
    }

    public static Integer translateToIndexOnList(ColumnReference cr,
                                                 ResultColumnList rcl,
                                                 ValueNode operand,
                                                 ResultSetNode resultSetNode,
                                                 BinaryRelationalOperatorNode brop,
                                                 JoinNode  joinNode)
            throws StandardException{
        Map<Pair<Integer, Integer>, ResultColumn> chainMap = ColumnUtils.rsnChainMap(rcl);
        boolean hasSubqueryNode = false;
        if (cr.getSource().getExpression() instanceof VirtualColumnNode) {
            VirtualColumnNode n = (VirtualColumnNode) cr.getSource().getExpression();
            if (n.getSourceResultSet() instanceof FromSubquery) {
                hasSubqueryNode = true;
            }
        }

        if (operand instanceof ColumnReference) {
            Pair<Integer, Integer> colCoord = ColumnUtils.RSCoordinate(cr.getSourceResultColumn() != null && !hasSubqueryNode ? cr.getSourceResultColumn() : cr.getOrigSourceResultColumn());
            if (chainMap.containsKey(colCoord)) {
                return chainMap.get(colCoord).getVirtualColumnId() - 1; // translate from 1-based to 0-based index
            } else {
                throw new RuntimeException(String.format("Unable to find ColRef %s in RCL %s", cr, rcl));
            }
        }
        else {
            try {
                LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();

                // Add a result column and return virtual column Id
                ResultColumn rc = cr.getSource();
                ResultSetNode rn = null;
                do {
                    VirtualColumnNode vn = (VirtualColumnNode) rc.getExpression();
                    rn = vn.getSourceResultSet();
                    rc = vn.getSourceColumn();
                } while (rn != resultSetNode);
                // construct a result column and add to result column list of child result set
                NodeFactory nodeFactory = lcc.getLanguageConnectionFactory().
                        getNodeFactory();

                ColumnReference generatedRef = (ColumnReference) nodeFactory.getNode(
                        C_NodeTypes.COLUMN_REFERENCE,
                        rc.getName(),
                        null,
                        ContextService.getService().getCurrentContextManager());

                assert rc.getExpression() instanceof VirtualColumnNode;
                VirtualColumnNode vn = (VirtualColumnNode)rc.getExpression();
                generatedRef.setSource(vn.getSourceColumn());

                if (operand instanceof TernaryOperatorNode) {
                    ((TernaryOperatorNode) operand).setReceiver(generatedRef);
                }
                else if (operand instanceof UnaryOperatorNode) {
                    ((UnaryOperatorNode) operand).setOperand(generatedRef);
                }
                ResultColumn resultColumn =
                        (ResultColumn) nodeFactory.getNode(C_NodeTypes.RESULT_COLUMN,
                                generatedRef.getColumnName(),
                                operand,
                                ContextService.getService().getCurrentContextManager());
                resultColumn.markGenerated();
                resultColumn.setResultSetNumber(resultSetNode.getResultSetNumber());
                resultColumn.setVirtualColumnId(rcl.size());
                rcl.addResultColumn(resultColumn);

                // Add a column to result column list of join node
                generatedRef = (ColumnReference) nodeFactory.getNode(
                        C_NodeTypes.COLUMN_REFERENCE,
                        rc.getName(),
                        null,
                        ContextService.getService().getCurrentContextManager());
                VirtualColumnNode vnode = (VirtualColumnNode) nodeFactory.getNode(C_NodeTypes.VIRTUAL_COLUMN_NODE,
                        resultSetNode, // source result set.
                        rc,
                        resultSetNode.getResultColumns().size(),
                        ContextService.getService().getCurrentContextManager());

                resultColumn =  (ResultColumn) nodeFactory.getNode(C_NodeTypes.RESULT_COLUMN,
                        rc.getName(),
                        vnode,
                        ContextService.getService().getCurrentContextManager());
                resultColumn.markGenerated();
                resultColumn.setResultSetNumber(joinNode.getResultSetNumber());
                generatedRef.setSource(resultColumn);
                if (brop.getLeftOperand() == operand) {
                    brop.setLeftOperand(generatedRef);
                }
                else {
                    brop.setRightOperand(generatedRef);
                }

                rebuildJoinNodeRCL(joinNode, resultSetNode, resultColumn);

            } catch (SQLException e) {
                throw StandardException.newException(e.getSQLState());
            }
            return rcl.size()-1;
        }
    }

    public static Pair<List<Integer>, List<Integer>> findHashIndices(final JoinNode node, Collection<Predicate> equiJoinPreds)
            throws StandardException {
        List<Integer> leftIndices = Lists.newArrayListWithCapacity(equiJoinPreds.size());
        List<Integer> rightIndices = Lists.newArrayListWithCapacity(equiJoinPreds.size());
        org.sparkproject.guava.base.Predicate<ResultColumn> isLeftRef = RSUtils.pointsTo(node.getLeftResultSet());
        ResultColumnList leftRCL = node.getLeftResultSet().getResultColumns();
        ResultColumnList rightRCL = node.getRightResultSet().getResultColumns();

        for (Predicate p : equiJoinPreds) {
            AndNode andNode = p.getAndNode();
            assert andNode.getLeftOperand() instanceof BinaryRelationalOperatorNode;
            BinaryRelationalOperatorNode brop = (BinaryRelationalOperatorNode)andNode.getLeftOperand();
            ValueNode leftOperand = brop.getLeftOperand();
            ValueNode rightOperand = brop.getRightOperand();
            List<ColumnReference> lcr = RSUtils.collectNodes(leftOperand, ColumnReference.class);
            List<ColumnReference> rcr = RSUtils.collectNodes(rightOperand, ColumnReference.class);

            for (ColumnReference cr : lcr) {
                if (isLeftRef.apply(cr.getSourceResultColumn()!=null?cr.getSourceResultColumn():cr.getOrigSourceResultColumn())) {
                    leftIndices.add(translateToIndexOnList(cr, leftRCL, leftOperand, node.getLeftResultSet(), brop, node));
                } else {
                    rightIndices.add(translateToIndexOnList(cr, rightRCL, leftOperand, node.getRightResultSet(), brop, node));
                }
            }

            for (ColumnReference cr : rcr) {
                if (isLeftRef.apply(cr.getSourceResultColumn()!=null?cr.getSourceResultColumn():cr.getOrigSourceResultColumn())) {
                    leftIndices.add(translateToIndexOnList(cr, leftRCL, rightOperand, node.getLeftResultSet(), brop, node));
                } else {
                    rightIndices.add(translateToIndexOnList(cr, rightRCL, rightOperand, node.getRightResultSet(), brop, node));
                }
            }
        }


        return Pair.of(leftIndices, rightIndices);
    }

    private static void rebuildJoinNodeRCL(JoinNode node,
                                           ResultSetNode child,
                                           ResultColumn rc) throws StandardException{

        boolean isLeft = node.getLeftResultSet() == child ? true : false;
        ResultColumnList rcl = node.getResultColumns();

        if (isLeft) {
            int size = child.getResultColumns().size()-1;
            ResultColumnList l = new ResultColumnList();
            while(rcl.size() > 0){
                l.addResultColumn(rcl.elementAt(0));
                rcl.removeElementAt(0);
            }
            for (int i = 0; i < size; ++i) {
                rcl.addResultColumn(l.elementAt(0));
                l.removeElementAt(0);
            }
            rcl.addResultColumn(rc);
            while(l.size() > 0) {
                rcl.addResultColumn(l.elementAt(0));
                l.removeElementAt(0);
            }
        }
        else {
            rcl.addResultColumn(rc);
        }
    }
}
