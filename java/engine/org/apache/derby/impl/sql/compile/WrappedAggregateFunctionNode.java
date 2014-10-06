package org.apache.derby.impl.sql.compile;

import java.util.Vector;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;

/**
 * @author Jeff Cunningham
 *         Date: 10/2/14
 */
public class WrappedAggregateFunctionNode extends WindowFunctionNode {
    private AggregateNode aggregateFunction;
    /**
     * Initializer. QueryTreeNode override.
     *
     * @param arg1 operands
     * @param arg2 The function's definition class
     * @throws org.apache.derby.iapi.error.StandardException
     */
    public void init(Object arg1, Object arg2) throws StandardException {
        super.init(arg1, null);
        aggregateFunction = (AggregateNode) arg2;
    }

    @Override
    public String getName() {
        return aggregateFunction.getAggregateName();
    }

    @Override
     public ValueNode getNewNullResultExpression() throws StandardException {
         return aggregateFunction.getNewNullResultExpression();
     }

    @Override
    public ValueNode[] getOperands() {
        return new ValueNode[] {operand};
    }

    @Override
    public ValueNode replaceCallWithColumnReference(ResultColumn rc, int tableNumber, int nestingLevel, ResultColumn
        newResultColumn) throws StandardException {
        ColumnReference node = (ColumnReference) aggregateFunction.replaceAggregatesWithColumnReferences(
            (ResultColumnList) getNodeFactory().getNode(
                C_NodeTypes.RESULT_COLUMN_LIST,
                getContextManager()),
            tableNumber);

        // Mark the ColumnReference as being generated to replace a call to
        // a window function
        node.markGeneratedToReplaceWindowFunctionCall();

        node.setSource(newResultColumn);
        rc.setExpression(node);
        return node;
    }

    /**
      * QueryTreeNode override. Prints the sub-nodes of this object.
      *
      * @param depth The depth of this node in the tree
      * @see QueryTreeNode#printSubNodes
      */
     public void printSubNodes(int depth) {
         if (SanityManager.DEBUG) {
             super.printSubNodes(depth);

             printLabel(depth, "aggregate: ");
             aggregateFunction.treePrint(depth + 1);
         }
     }

     @Override
      public ValueNode replaceAggregatesWithColumnReferences(ResultColumnList rcl,
                                                             int tableNumber) throws StandardException {
          return aggregateFunction.replaceAggregatesWithColumnReferences(rcl,tableNumber);
      }

    @Override
    public ValueNode bindExpression(FromList fromList, SubqueryList subqueryList, Vector aggregateVector) throws
        StandardException {
        return aggregateFunction.bindExpression(fromList, subqueryList, aggregateVector);
    }

    @Override
    public int hashCode() {
        return aggregateFunction.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return aggregateFunction.equals(obj);
    }

    /**
     * Get the generated ResultColumn where this
     * aggregate now resides after a call to
     * replaceAggregatesWithColumnReference().
     *
     * @return the result column
     */
     @Override
     AggregateDefinition getAggregateDefinition() {
         return aggregateFunction.getAggregateDefinition();
     }
     @Override
     public boolean isDistinct() {
         return aggregateFunction.isDistinct();
     }

     @Override
     public String getAggregatorClassName() {
         return aggregateFunction.getAggregatorClassName();
     }

     @Override
     public String getAggregateName() {
         return aggregateFunction.getAggregateName();
     }

     @Override
     public ResultColumn getNewAggregatorResultColumn(DataDictionary dd) throws StandardException {
         return aggregateFunction.getNewAggregatorResultColumn(dd);
     }

     @Override
     public ResultColumn getNewExpressionResultColumn(DataDictionary dd) throws StandardException {
         return aggregateFunction.getNewExpressionResultColumn(dd);
     }

     @Override
     public void generateExpression(ExpressionClassBuilder acb, MethodBuilder mb) throws StandardException {
         aggregateFunction.generateExpression(acb, mb);
     }

     @Override
     public String getSQLName() {
         return aggregateFunction.getSQLName();
     }

     @Override
     public ColumnReference getGeneratedRef() {
         return aggregateFunction.getGeneratedRef();
     }

     @Override
     public ResultColumn getGeneratedRC() {
         return aggregateFunction.getGeneratedRC();
     }

}
