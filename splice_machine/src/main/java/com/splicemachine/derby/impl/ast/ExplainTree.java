package com.splicemachine.derby.impl.ast;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Iterators;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;

import java.util.*;

/**
 * Representation of a Query Plan as a Tree. This uses Nodes which point to "children" and to "subqueries".
 *
 * This is significantly easier to work with than a map with random garbage goodies.
 *
 * @author Scott Fines
 *         Date: 4/9/15
 */
public class ExplainTree{
    private final Node topNode;
    private static final Function<? super Node,? extends String> toStringFunction = new Function<Node, String>(){
        @Override
        public String apply(Node node){
            return node.toString();
        }
    };

    private ExplainTree(Node topNode){
        this.topNode = topNode;
    }

    public Iterator<String> treeToString(){
        return Iterators.transform(new Tree(topNode),toStringFunction);
    }

    public static class Builder{
        protected Node topNode;

        public void reset(){
            topNode = null;
        }

        public Builder pushJoin(int rsNum,
                                CostEstimate ce,
                                String joinStrategy,
                                List<String> predicates,
                                Builder rightSide){
            JoinNode jn = new JoinNode(rsNum,ce,predicates,joinStrategy);
            jn.inner = rightSide.topNode;
            rightSide.incrementLevel();
            assert topNode!=null: "Must push over left side!";
            jn.outer = topNode;
            jn.level = topNode.level;
            incrementLevel();
            topNode = jn;
            return this;
        }

        public Builder pushSubquery(int rsNum,boolean exprSub,boolean corrSub,boolean invariant,Builder subqueryBuilder){
            //push up the subquery side
            subqueryBuilder.incrementLevel(2);
            SubqueryNode subqueryNode = new SubqueryNode(rsNum,exprSub,corrSub,invariant);
            subqueryNode.child = subqueryBuilder.topNode;
            subqueryNode.refNode = topNode;
            topNode.addSubquery(subqueryNode);
            return this;
        }

        public void pushTableOperator(String nodeName,int rsNum,CostEstimate ce,Builder rightBuilder){
            TwoChildPredicatedNode tableOperator = new TwoChildPredicatedNode(nodeName,rsNum,ce,null);
            rightBuilder.incrementLevel();
            assert topNode!=null:"Must push over left side!";
            tableOperator.outer = topNode;
            tableOperator.inner = rightBuilder.topNode;
            tableOperator.level = topNode.level;
            incrementLevel();
            topNode = tableOperator;
        }

        private void incrementLevel(){
            incrementLevel(1);
        }
        private void incrementLevel(int count){
            Iterator<Node> nodeIterator;
            nodeIterator = allNodes();
            while(nodeIterator.hasNext())
                nodeIterator.next().level+=count;
        }

        public void pushValuesNode(int rsNum,CostEstimate ce){
            Node n = new Node("Values",rsNum,ce);
            topNode = n;
            n.level = 0;
        }

        private Iterator<Node> allNodes(){
           return new Tree(topNode);
        }

        public Builder addBaseTable(int resultSetNumber,
                                    CostEstimate cost,
                                    String tableName,
                                    String indexName,
                                    List<String> predicates){
            Node newNode = new TableNode(resultSetNumber,cost,predicates,tableName,indexName);
            topNode = newNode;
            newNode.level = 0;
            return this;
        }

        public Builder pushProjection(int resultSetNumber, CostEstimate costEstimate,List<String> predicates){
            SingleChildPredicateNode projectRestrict = new SingleChildPredicateNode("ProjectRestrict",resultSetNumber,costEstimate,predicates);
            pushBuiltNode(projectRestrict);
            return this;
        }

        public Builder pushIndexFetch(int rsNum,CostEstimate ce,long heapTable){
            SingleChildPredicateNode projectRestrict = new SingleChildPredicateNode("IndexLookup",rsNum,ce,Collections.<String>emptyList());
            pushBuiltNode(projectRestrict);
            return this;

        }

        public ExplainTree build(){
            return new ExplainTree(topNode);
        }

        public Builder pushNode(String nodeName,int rsNum,CostEstimate ce){
            SingleChildPredicateNode projectRestrict = new SingleChildPredicateNode(nodeName,rsNum,ce,Collections.<String>emptyList());
            pushBuiltNode(projectRestrict);
            return this;
        }

        private void pushBuiltNode(SingleChildPredicateNode projectRestrict){
            assert topNode!=null:" Programmer error: cannot push an index lookup over a null explain node!";
            projectRestrict.child=topNode;
            projectRestrict.level=topNode.level;
            incrementLevel();
            topNode = projectRestrict;
        }
    }

    private static class Tree implements Iterator<Node> {
        private Node next;
        private LinkedList<TwoChildPredicatedNode> joinStack;
        private LinkedList<SubqueryNode> subqs;

        public Tree(Node next){
            this.next=next;
        }

        @Override
        public boolean hasNext(){
            return next!=null;
        }

        @Override
        public Node next(){
            if(!hasNext()) throw new NoSuchElementException();
            Node n = next;
            advance();
            return n;
        }

        @Override public void remove(){ throw new UnsupportedOperationException(); }

        private void advance(){
            if(next instanceof TwoChildPredicatedNode){
                if(joinStack==null) joinStack = new LinkedList<>();
                joinStack.addFirst((TwoChildPredicatedNode)next);
            }
            List<SubqueryNode> subqueries = next.getSubqueries();
            if(subqueries!=null){
                if(subqs==null)
                    subqs = new LinkedList<>();
                if(subqs.size()>0)
                    subqs.addAll(0,subqueries);
                else
                    subqs.addAll(subqueries);
            }

            next = next.nextNode();
            /*
             * Before we pop up a level, we see if there is a subquery to check
             */
            if(next==null && subqs!=null && subqs.size()>0)
                next = subqs.removeFirst();
            //pop a join element off the stack and go down the right side
            while(next==null && joinStack!=null && joinStack.size()>0){
                TwoChildPredicatedNode jn = joinStack.removeFirst();
                next=jn.nextNode(); //go down the right hand side
            }
        }
    }

    private static class Node{
        private CostEstimate cost;
        protected int resultSetNumber;
        int level;
        private String className;
        private List<SubqueryNode> subqueries;

        public Node(String className, int resultSetNumber,CostEstimate cost){
            this.cost=cost;
            this.resultSetNumber=resultSetNumber;
            this.className=className;
        }

        @Override
        public String toString(){
            StringBuilder sb = new StringBuilder();
            sb = sb.append(spaceToLevel())
                    .append(className).append("(")
                    .append("n=").append(resultSetNumber)
                    .append(",cost=").append(cost.prettyString());
            String extras = getExtraInformation();
            if(extras!=null)
                sb = sb.append(",").append(extras);
            sb = sb.append(")");
            return sb.toString();
        }

        private static final String spaces="  ";

        public void addSubquery(SubqueryNode subqueryNode){
            if(subqueries==null)
                subqueries = new ArrayList<>(1);
            subqueries.add(subqueryNode);
        }

        protected String spaceToLevel(){
            if(level==0) return "";
            else
                return Strings.repeat(spaces,level-1)+"->"+spaces;
        }

        protected String getExtraInformation(){
            //by default, return nothing else
            return null;
        }

        Node nextNode(){
            return null;
        }

        public List<SubqueryNode> getSubqueries(){
            return subqueries;
        }

    }

    private static abstract class PredicatedNode extends Node{
        private final List<String> predicateStrings;

        public PredicatedNode(String className,
                              int resultSetNUmber,
                              CostEstimate cost,
                              List<String> predicateStrings){
            super(className,resultSetNUmber,cost);
            this.predicateStrings=predicateStrings;
        }

        @Override
        protected String getExtraInformation(){
            if(predicateStrings==null || predicateStrings.size()<=0) return null; //nothing to add
            return "preds=["+Joiner.on(",").skipNulls().join(predicateStrings)+"]";
        }
    }

    private static class SingleChildPredicateNode extends PredicatedNode{
        Node child;
        public SingleChildPredicateNode(String className,
                                        int resultSetNUmber,
                                        CostEstimate cost,
                                        List<String> predicateStrings){
            super(className,resultSetNUmber,cost,predicateStrings);
        }

        @Override
        Node nextNode(){
            return child;
        }
    }


    private static class TwoChildPredicatedNode extends PredicatedNode{
        protected Node outer;
        protected Node inner;

        private boolean atInner = true;

        public TwoChildPredicatedNode(String className,int resultSetNUmber,CostEstimate cost,List<String> predicateStrings){
            super(className,resultSetNUmber,cost,predicateStrings);
        }

        @Override
        Node nextNode(){
            if(atInner) {
                atInner = false;
                return inner;
            } else {
                atInner = true;
                return outer;
            }
        }

    }
    private static class JoinNode extends TwoChildPredicatedNode{
        private String joinStrategy;

        public JoinNode(int resultSetNUmber,
                        CostEstimate cost,
                        List<String> predicateStrings,
                        String joinStrategy){
            super("Join",resultSetNUmber,cost,predicateStrings);
            this.joinStrategy=joinStrategy;
        }

        @Override
        protected String getExtraInformation(){
            StringBuilder sb = new StringBuilder();
            sb = sb.append("exe=").append(joinStrategy)
                    .append(",").append(super.getExtraInformation());

            return sb.toString();
        }
    }

    private static class SubqueryNode extends Node{
        private Node child;
        private boolean expression;
        private boolean invariant;
        private boolean correlated;
        private Node refNode;

        public SubqueryNode(int resultSetNumber,
                            boolean expression,
                            boolean invariant,
                            boolean correlated){
            super("Subquery",resultSetNumber,null);
            this.expression=expression;
            this.invariant=invariant;
            this.correlated=correlated;
        }

        @Override
        public String toString(){
            StringBuilder sb = new StringBuilder();
            level = refNode.level;
            sb = sb.append(spaceToLevel())
                    .append("Subquery (")
                    .append("n=").append(resultSetNumber)
                    .append(",").append(getExtraInformation()).append(")");
            return sb.toString();
        }

        @Override
        protected String getExtraInformation(){
            return String.format("correlated?%b, expression?%b, invariant?%b",
                    expression,invariant,correlated);
        }

        @Override
        Node nextNode(){
            return child;
        }

    }

    private static class TableNode extends PredicatedNode{
        private final String tableName;
        private final String indexName;

        public TableNode(int resultSetNUmber,
                         CostEstimate cost,
                         List<String> predicateStrings,
                         String tableName,
                         String indexName){
            super(getClassName(indexName),resultSetNUmber,cost,predicateStrings);
            this.tableName=tableName;
            this.indexName=indexName;
        }

        private static String getClassName(String indexName){
            if(indexName!=null) return "IndexScan";
            return "TableScan";
        }

        @Override
        protected String getExtraInformation(){
            StringBuilder sb = new StringBuilder();
            if(tableName!=null)
                sb = sb.append("table=").append(tableName);
            if(indexName!=null){
                if(tableName!=null)
                    sb = sb.append(",");
                sb=sb.append("using-index=").append(indexName);
            }

            String superInfo = super.getExtraInformation();
            if(superInfo==null) return sb.toString();

            sb = sb.append(",").append(super.getExtraInformation());
            return sb.toString();
        }
    }
}
