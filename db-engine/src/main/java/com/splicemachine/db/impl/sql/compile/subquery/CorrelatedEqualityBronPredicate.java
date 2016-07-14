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

package com.splicemachine.db.impl.sql.compile.subquery;

import com.splicemachine.db.impl.sql.compile.BinaryRelationalOperatorNode;
import com.splicemachine.db.impl.sql.compile.ColumnReference;
import com.splicemachine.db.impl.sql.compile.RelationalOperator;
import org.sparkproject.guava.base.Predicate;

/**
 * A predicate that evaluates to true if a given BinaryRelationalOperatorNode has this shape:
 * <pre>
 * BRON(=)
 *  /  \
 * CR  CR
 * </pre>
 *
 * Where ONLY one of the CR is correlated with a nesting level equal to that specified in the constructor. This can be
 * used to find correlated equality predicates in a subquery that are referencing one level up.
 */
public class CorrelatedEqualityBronPredicate implements Predicate<BinaryRelationalOperatorNode> {

    private int sourceLevel;

    public CorrelatedEqualityBronPredicate(int sourceLevel) {
        this.sourceLevel = sourceLevel;
    }

    @Override
    public boolean apply(BinaryRelationalOperatorNode bron) {
        if (bron.getOperator() != RelationalOperator.EQUALS_RELOP) {
            return false;
        }

        if (!(bron.getLeftOperand() instanceof ColumnReference) || !(bron.getRightOperand() instanceof ColumnReference)) {
            return false;
        }

        ColumnReference left = (ColumnReference) bron.getLeftOperand();
        ColumnReference right = (ColumnReference) bron.getRightOperand();
        return (!left.getCorrelated() && right.getCorrelated() && right.getSourceLevel() == sourceLevel)
                ||
                (!right.getCorrelated() && left.getCorrelated() && left.getSourceLevel() == sourceLevel);
    }


}
