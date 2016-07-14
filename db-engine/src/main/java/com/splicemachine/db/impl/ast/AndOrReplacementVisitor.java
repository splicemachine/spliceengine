/*
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.impl.ast;

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
