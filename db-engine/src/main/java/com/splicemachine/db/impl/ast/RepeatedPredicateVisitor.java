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
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.impl.sql.compile.AndNode;
import com.splicemachine.db.impl.sql.compile.BinaryListOperatorNode;
import com.splicemachine.db.impl.sql.compile.BinaryRelationalOperatorNode;
import com.splicemachine.db.impl.sql.compile.OrNode;
import com.splicemachine.db.impl.sql.compile.ValueNode;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * This Visitor finds the first ValueNode (which is typically an Or/And node and attempts
 * to find duplicate predicates.  A good example is a join condition contained in every path
 * of the where clause, such as:
 *
 * select *
 * from table1 t1, table2 t2
 * where { {
 *           t1.foo = t2.foo
 *           and t1.bar = 'bar'
 *           and t2.baz = 'baz'
 *
 *         } OR {
 *
 *           t1.foo = t2.foo
 *           and t1.bar = 'not bar'
 *           and t2.baz = 'not baz'
 *
 *          } OR {
 *
 *           t1.foo = t2.foo
 *           and t1.bar = 'really bar'
 *           and t2.baz = 'really baz'
 *
 *          }
 *       }
 *
 * Derby's optimizer doesn't currently spot the repeated join condition and thus will keep the whole predicate
 * together.  This does not allow the join condition to be pushed down to the right side and will then not
 * allow a Merge Sort Join to be used.  This code will spot the repeated BinaryRelationalOperation (i.e. t1.foo = t2.foo)
 * and break that out from the ORs, move it to an and clause, so t1.foo = t2.foo AND {...} OR {...} OR {...}
 *
 */
public class RepeatedPredicateVisitor extends AbstractSpliceVisitor {

    private boolean foundWhereClause = false;

    /**
     * Finds any node in the predicate part of the query tree that occurs more than once.
     *
     * @param pred
     * @return a map of the predicate ValueNode and how many times it occurs (only including ValueNodes that occur more than once
     * @throws StandardException
     */
    private Map<ValueNode,Integer> nodesWithMultipleOccurrences(ValueNode pred) throws StandardException {

        Map<ValueNode, Integer> m = new HashMap<ValueNode,Integer>();

        List<ValueNode> binNodes = new LinkedList<ValueNode>();
        binNodes.addAll(RSUtils.collectNodes(pred, BinaryRelationalOperatorNode.class));
        binNodes.addAll(RSUtils.collectNodes(pred, BinaryListOperatorNode.class));

        for(ValueNode node : binNodes ){

            if(m.containsKey(node)){
                Integer count = m.get(node);
                m.put(node, Integer.valueOf( count.intValue() + 1));
            }else{
                m.put(node, Integer.valueOf(1));
            }
        }

        Iterator<Map.Entry<ValueNode, Integer>> mapIt = m.entrySet().iterator();

        while(mapIt.hasNext()){

            Map.Entry<ValueNode,Integer> me = mapIt.next();
            Integer i = me.getValue();

            if(i.intValue() <= 1){
                mapIt.remove();
            }
        }

        return m;
    }

    private boolean foundInPath(ValueNode nodeToFind, ValueNode predNode){

        boolean result = false;

        if(nodeToFind.equals(predNode)){

            result = true;

        }else if(predNode instanceof OrNode){

            OrNode orNode = (OrNode) predNode;
            result = foundInPath(nodeToFind, orNode.getLeftOperand())
                    && foundInPath(nodeToFind, orNode.getRightOperand());

        }else if(predNode instanceof AndNode){

            AndNode andNode = (AndNode) predNode;
            result = foundInPath(nodeToFind, andNode.getLeftOperand())
                    || foundInPath(nodeToFind, andNode.getRightOperand());
        }


        return result;
    }

    /**
     * For a given node, check each possible path to ensure that the possibly duplicated
     * predicate occurs down that path.  If it is found down each path in the plan, it
     * can be pulled out, then 'anded' with the rest of the predicate(s)
     */
    @Override
    public Visitable defaultVisit(Visitable node) throws StandardException {

        Visitable updatedNode = node;
        // DB-5672: Disable repeated predicate elimination because it does not work in general. Much
        // more work needs to be done to make sure the predicates are equivalent before and after transformation.
        //
        /*if(node instanceof ValueNode){

            foundWhereClause = true;

            ValueNode newNode = (ValueNode) node;

            Map<ValueNode, Integer> m = nodesWithMultipleOccurrences(newNode);

            for(Map.Entry<ValueNode,Integer> me : m.entrySet()){
                if(foundInPath(me.getKey(), newNode)){
                    AndOrReplacementVisitor aor = new AndOrReplacementVisitor(me.getKey());
                    newNode.accept(new SpliceDerbyVisitorAdapter(aor));
                    AndNode newAndNode = (AndNode) ((ValueNode) node).getNodeFactory().getNode(
                            C_NodeTypes.AND_NODE,
                            me.getKey(),
                            newNode,
                            ((ValueNode) node).getContextManager());
                    newNode = newAndNode;
                }


            updatedNode = newNode;
        }*/

        return updatedNode;
    }


    @Override
    public boolean isPostOrder() {
        return false;
    }

    @Override
    public boolean skipChildren(Visitable node) {
       return foundWhereClause;
    }
}
