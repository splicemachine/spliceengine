package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import java.util.Collection;

/**
 * BatchOnceNode replaces a ProjectRestrictNode below an update that would otherwise invoke a Subquery for each row from
 * the source.  BatchOnce reads multiple rows from the source then executes the subquery once.
 *
 * Example Query: update A set A.name = (select B.name from B where B.id=A.id)
 *
 * <pre>
 *  BEFORE:
 *
 * Update -> ProjectRestrict -> ChildResultSet
 *              \
 *              \
 *             Subquery
 *
 *  AFTER:
 *
 * Update -> BatchOnceNode -> ChildResultSet
 *              \
 *              \
 *             Subquery
 * </pre>
 */
public class BatchOnceNode extends SingleChildResultSetNode {

    private SubqueryNode subqueryNode;

    /* The column position in the source of a RowLocation column (or -1 if the source does not provide) */
    private int sourceRowLocationColumnPosition;

    /* The column position in the source referenced by the correlated subquery column reference */
    private int sourceCorrelatedColumnPosition;

    /* The column position of the correlated column in the subquery result set */
    private int subqueryCorrelatedColumnPosition;


    /* Used by NodeFactory */
    public BatchOnceNode() {
    }

    @Override
    public void init(Object projectRestrictNode,
                     Object subqueryNode,
                     Object sourceRowLocationColumnPosition,
                     Object sourceCorrelatedColumnPosition,
                     Object subqueryCorrelatedColumnPosition
    ) {
        this.childResult = (ResultSetNode) projectRestrictNode;
        this.subqueryNode = (SubqueryNode) subqueryNode;
        this.sourceRowLocationColumnPosition = (int) sourceRowLocationColumnPosition;
        this.sourceCorrelatedColumnPosition = (int) sourceCorrelatedColumnPosition;
        this.subqueryCorrelatedColumnPosition = (int) subqueryCorrelatedColumnPosition;
    }

    @Override
    protected void generate(ActivationClassBuilder acb, MethodBuilder mb) throws StandardException {

        // push: method call to get ResultSetFactory
        acb.pushGetResultSetFactoryExpression(mb);

        // push: parameters to method of ResultSetFactory
        this.childResult.generate(acb, mb);                      // ARG 1 - childResultSet
        acb.pushThisAsActivation(mb);                            // ARG 2 - activation
        mb.push(getCompilerContext().getNextResultSetNumber());  // ARG 3 - resultSetNumber
        this.subqueryNode.getResultSet().generate(acb, mb);      // ARG 4 - the subquery result set
        mb.push(acb.getRowLocationScanResultSetName());          // ARG 5 - name of the activation field containing update RS
        mb.push(sourceRowLocationColumnPosition);                // ARG 6 - position of the row location column in the source RS
        mb.push(sourceCorrelatedColumnPosition);                 // ARG 7 - position of the correlated CR in the update source RS
        mb.push(subqueryCorrelatedColumnPosition);               // ARG 8 - position of the correlated CR in subquery RS

        // push: method to invoke on ResultSetFactory
        mb.callMethod(VMOpcode.INVOKEINTERFACE, null, "getBatchOnceResultSet", ClassName.NoPutResultSet, 8);
    }


    @Override
    public ResultColumnDescriptor[] makeResultDescriptors() {
        return childResult.makeResultDescriptors();
    }

    @Override
    public ResultColumnList getResultColumns() {
        return this.childResult.getResultColumns();
    }

    @Override
    public void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);
        subqueryNode = (SubqueryNode) subqueryNode.accept(v);
    }


    @Override
    public String printExplainInformation(String attrDelim, int order) throws StandardException {
        StringBuilder sb = new StringBuilder();
        sb.append(spaceToLevel())
                .append("BatchOnce").append("(")
                .append("n=").append(order)
                .append(attrDelim).append(getFinalCostEstimate().prettyProcessingString());
        sb.append(")");
        return sb.toString();
    }
    @Override
    public void buildTree(Collection<QueryTreeNode> tree, int depth) throws StandardException {
        setDepth(depth);
        tree.add(this);
        subqueryNode.buildTree(tree,depth+1);
        childResult.buildTree(tree,depth+1);
    }

}
