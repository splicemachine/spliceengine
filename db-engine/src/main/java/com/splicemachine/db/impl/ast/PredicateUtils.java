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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.ast;

import com.splicemachine.db.iapi.sql.compile.OptimizablePredicate;
import com.splicemachine.db.impl.sql.compile.*;
import org.spark_project.guava.base.Function;
import java.util.ArrayList;
import java.util.List;

/**
 * @author P Trolard
 *         Date: 18/10/2013
 */
public class PredicateUtils {

    public static org.spark_project.guava.base.Predicate<Predicate> isEquiJoinPred = new org.spark_project.guava.base.Predicate<Predicate>() {
        @Override
        public boolean apply(Predicate p) {
            return p != null &&
                    p.isJoinPredicate() &&
                    p.getAndNode().getLeftOperand().isBinaryEqualsOperatorNode();
        }
    };

    public static org.spark_project.guava.base.Predicate<Predicate> isJoinPred = new org.spark_project.guava.base.Predicate<Predicate>() {
        @Override
        public boolean apply(Predicate p) {
            return p != null &&
                    p.isJoinPredicate();
        }
    };

    /**
     * Return string representation of Derby Predicate
     */
    public static Function<Predicate, String> predToString = new Function<Predicate, String>() {
        @Override
        public String apply(Predicate predicate) {
            if (predicate == null) {
                return null;
            }
            ValueNode operand = predicate.getAndNode().getLeftOperand();
            return com.splicemachine.db.impl.sql.compile.OperatorToString.opToString(operand);
        }
    };

    /**
     * Return string representation of Derby PredicateList
     */
    public static Function<PredicateList, String> predListToString = new Function<PredicateList, String>() {
        @Override
        public String apply(PredicateList predicateList) {
            if (predicateList == null) {
                return null;
            }
            StringBuilder buf = new StringBuilder();
            for (int i = 0, s = predicateList.size(); i < s; i++) {
                OptimizablePredicate predicate = predicateList.getOptPredicate(i);
                ValueNode operand = ((Predicate) predicate).getAndNode().getLeftOperand();
                buf.append(OperatorToString.opToString(operand)).append(", ");
            }
            if (buf.length() > 2) {
                // trim last ", "
                buf.setLength(buf.length() - 2);
            }
            return buf.toString();
        }
    };

    /**
     * Return a List of Predicates for a Derby PredicateList
     */
    public static List<Predicate> PLtoList(PredicateList pl) {
        if (pl==null)
            return new ArrayList<Predicate>();
        List<Predicate> preds = new ArrayList<>(pl.size());
        for (int i = 0, s = pl.size(); i < s; i++) {
            OptimizablePredicate p = pl.getOptPredicate(i);
            preds.add((Predicate) p);
        }
        return preds;
    }


    /**
     * TRUE if the left operation is a ColumnReference with the specified nesting level.
     */
    public static boolean isLeftColRef(BinaryRelationalOperatorNode pred, int atSourceLevel) {
        if (!(pred.getLeftOperand() instanceof ColumnReference)) {
            return false;
        }
        ColumnReference left = (ColumnReference) pred.getLeftOperand();
        return left.getSourceLevel() == atSourceLevel;
    }

    /**
     * TRUE if the right operation is a ColumnReference with the specified nesting level.
     */
    public static boolean isRightColRef(BinaryRelationalOperatorNode pred, int atSourceLevel) {
        if (!(pred.getRightOperand() instanceof ColumnReference)) {
            return false;
        }
        ColumnReference right = (ColumnReference) pred.getRightOperand();
        return right.getSourceLevel() == atSourceLevel;
    }


}
