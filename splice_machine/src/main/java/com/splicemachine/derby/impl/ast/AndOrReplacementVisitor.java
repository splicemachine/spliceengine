package com.splicemachine.derby.impl.ast;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.impl.sql.compile.AndNode;
import com.splicemachine.db.impl.sql.compile.BinaryOperatorNode;
import com.splicemachine.db.impl.sql.compile.OrNode;
import com.splicemachine.db.impl.sql.compile.ValueNode;

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
