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
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.OptimizablePredicate;

import static java.lang.String.format;

/**
 * Utility to get the string representation of a given operator.
 * <p/>
 * Used for debugging.
 */
public class OperatorToString {


    /**
     * Satisfy non-guava (derby client) compile dependency.
     * @param predicate the predicate
     * @return Return string representation of Derby Predicate
     */
    public static String toString(Predicate predicate) {
        if (predicate == null) {
            return null;
        }
        ValueNode operand = predicate.getAndNode().getLeftOperand();
        return opToString(operand);
    }

    /**
     * Satisfy non-guava (derby client) compile dependency.
     * @param predicateList the predicate list
     * @return Return string representation of Derby Predicates in a predicate list
     */
    public static String toString(PredicateList predicateList) {
        if (predicateList == null || predicateList.isEmpty()) {
            return null;
        }
        StringBuilder buf = new StringBuilder();
        for (int i = 0, s = predicateList.size(); i < s; i++) {
            OptimizablePredicate predicate = predicateList.getOptPredicate(i);
            ValueNode operand = ((Predicate)predicate).getAndNode().getLeftOperand();
            buf.append(opToString(operand)).append(", ");
        }
        if (buf.length() > 2) {
            // trim last ", "
            buf.setLength(buf.length() - 2);
        }
        return buf.toString();
    }

    /**
     * Return string representation of a Derby expression
     */
    public static String opToString(ValueNode operand){
        if(operand==null){
            return "";
        }else if(operand instanceof UnaryOperatorNode){
            UnaryOperatorNode uop=(UnaryOperatorNode)operand;
            return format("%s(%s)",uop.getOperatorString(),opToString(uop.getOperand()));
        }else if(operand instanceof BinaryRelationalOperatorNode){
            BinaryRelationalOperatorNode bron=(BinaryRelationalOperatorNode)operand;
            try {
                InListOperatorNode inListOp = bron.getInListOp();
                if (inListOp != null) return opToString(inListOp);
    
                return format("(%s %s %s)", opToString(bron.getLeftOperand()),
                    bron.getOperatorString(), opToString(bron.getRightOperand()));
            }
            catch (StandardException e) {
                return "PARSE_ERROR_WHILE_CONVERTING_OPERATOR";
            }
        }else if(operand instanceof BinaryListOperatorNode){
            BinaryListOperatorNode blon = (BinaryListOperatorNode)operand;
            StringBuilder inList = new StringBuilder("(");
            if (!blon.isSingleLeftOperand()) {
                ValueNodeList vnl = blon.leftOperandList;
                inList.append("(");
                for (int i = 0; i < vnl.size(); i++) {
                    ValueNode vn = (ValueNode) vnl.elementAt(i);
                    if (i != 0)
                        inList.append(",");
                    inList.append(opToString(vn));
                }
                inList.append(")");
            }
            else
                inList.append(opToString(blon.getLeftOperand()));
            inList.append(" ").append(blon.getOperator()).append(" (");
            ValueNodeList rightOperandList=blon.getRightOperandList();
            boolean isFirst = true;
            for(Object qtn: rightOperandList){
                if(isFirst) isFirst = false;
                else inList = inList.append(",");
                inList = inList.append(opToString((ValueNode)qtn));
            }
            return inList.append("))").toString();
        }else if (operand instanceof BinaryOperatorNode) {
            BinaryOperatorNode bop = (BinaryOperatorNode) operand;
            return format("(%s %s %s)", opToString(bop.getLeftOperand()),
                          bop.getOperatorString(), opToString(bop.getRightOperand()));
        } else if (operand instanceof ArrayOperatorNode) {
            ArrayOperatorNode array = (ArrayOperatorNode) operand;
            ValueNode op = array.operand;
            return format("%s[%d]", op == null ? "" : opToString(op), array.extractField);
        } else if (operand instanceof TernaryOperatorNode) {
            TernaryOperatorNode top = (TernaryOperatorNode) operand;
            ValueNode rightOp = top.getRightOperand();
            return format("%s(%s, %s%s)", top.getOperator(), opToString(top.getReceiver()),
                          opToString(top.getLeftOperand()), rightOp == null ? "" : ", " + opToString(rightOp));
        }
        else if (operand instanceof ArrayConstantNode) {
            ArrayConstantNode arrayConstantNode = (ArrayConstantNode) operand;
            StringBuilder builder = new StringBuilder();
            builder.append("[");
            int i = 0;
            for (Object object: arrayConstantNode.argumentsList) {
                if (i!=0)
                    builder.append(",");
                builder.append(opToString((ValueNode)object));
                i++;
            }
            builder.append("]");
            return builder.toString();
        } else if (operand instanceof ListValueNode) {
            ListValueNode lcn = (ListValueNode) operand;
            StringBuilder builder = new StringBuilder();
            builder.append("(");
            for (int i = 0; i < lcn.numValues(); i++) {
                ValueNode vn = lcn.getValue(i);
                if (i != 0)
                    builder.append(",");
                builder.append(opToString(vn));
            }
            builder.append(")");
            return builder.toString();
        }
        else if (operand instanceof ColumnReference) {
            ColumnReference cr = (ColumnReference) operand;
            String table = cr.getTableName();
            ResultColumn source = cr.getSource();
            return format("%s%s%s",table==null?"":format("%s.",table),
                          cr.getColumnName(),source==null?"":
                              format("[%s:%s]",source.getResultSetNumber(),source.getVirtualColumnId()));
        } else if (operand instanceof VirtualColumnNode) {
            VirtualColumnNode vcn = (VirtualColumnNode) operand;
            ResultColumn source = vcn.getSourceColumn();
            String table = source.getTableName();
            return format("%s%s%s", table == null ? "" : format("%s.", table),
                          source.getName(),
                          format("[%s:%s]", source.getResultSetNumber(), source.getVirtualColumnId()));
        } else if (operand instanceof SubqueryNode) {
            SubqueryNode subq = (SubqueryNode) operand;
            return format("subq=%s", subq.getResultSet().getResultSetNumber());
        } else if (operand instanceof ConstantNode) {
            ConstantNode cn = (ConstantNode) operand;
            try {
                return cn.getValue().getString();
            } catch (StandardException se) {
                return se.getMessage();
            }
        } else if(operand instanceof CastNode){
            return opToString(((CastNode)operand).getCastOperand());
        } else{
            return replace(operand.toString(), "\n", " ");
        }
    }


    private static String replace(String text, String searchString, String replacement) {
        return replace(text, searchString, replacement, -1);
    }

    private static String replace(String text, String searchString, String replacement, int max) {
        if (text.isEmpty() || searchString.isEmpty() || replacement == null || max == 0) {
            return text;
        }
        int start = 0;
        int end = text.indexOf(searchString, start);
        if (end == -1) {
            return text;
        }
        int replLength = searchString.length();
        int increase = replacement.length() - replLength;
        increase = (increase < 0 ? 0 : increase);
        increase *= (max < 0 ? 16 : (max > 64 ? 64 : max));
        StringBuilder buf = new StringBuilder(text.length() + increase);
        while (end != -1) {
            buf.append(text.substring(start, end)).append(replacement);
            start = end + replLength;
            if (--max == 0) {
                break;
            }
            end = text.indexOf(searchString, start);
        }
        buf.append(text.substring(start));
        return buf.toString();
    }

}
