package com.splicemachine.derby.impl.ast;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.impl.sql.compile.AndNode;
import org.apache.derby.impl.sql.compile.BinaryOperatorNode;
import org.apache.derby.impl.sql.compile.OrNode;
import org.apache.derby.impl.sql.compile.ValueNode;

public class AndOrReplacementVisitor extends AbstractSpliceVisitor {

    private ValueNode nodeToReplace;

    public AndOrReplacementVisitor(ValueNode nodeToReplace){
        this.nodeToReplace = nodeToReplace;
    }

    @Override
    public Visitable visit(OrNode node) throws StandardException {
       return visitBinaryOperator(node);
    }

    @Override
    public Visitable visit(AndNode node) throws StandardException {
        return visitBinaryOperator(node);
    }

    private Visitable visitBinaryOperator(BinaryOperatorNode bon) throws StandardException {

        Visitable replacementNode = null;

        if(nodeToReplace.equals(bon.getLeftOperand())){
            replacementNode = bon.getRightOperand();
        }else if(nodeToReplace.equals(bon.getRightOperand())){
            replacementNode = bon.getLeftOperand();
        }else{
            replacementNode = bon;
        }

        return replacementNode;
    }
}
