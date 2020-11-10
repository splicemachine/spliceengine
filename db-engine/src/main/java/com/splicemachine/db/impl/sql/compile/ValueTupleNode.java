package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.util.JBitSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.ArrayList;
import java.util.List;

/**
 * A ValueTupleNode represents a tuple of ValueNodes that can be used for example in
 * select * from t where (a,b) = (0,0)
 *
 */
@SuppressFBWarnings(value="HE_INHERITS_EQUALS_USE_HASHCODE", justification="DB-9277")
public class ValueTupleNode extends ValueNode {
    private List<ValueNode> tuple = new ArrayList<>();

    @Override
    protected boolean isEquivalent(ValueNode o) throws StandardException {
        if (!isSameNodeType(o)) {
            return false;
        }
        ValueTupleNode other = (ValueTupleNode) o;
        if (other.tuple.size() != tuple.size()) {
            return false;
        }

        for (int i = 0; i < tuple.size(); i++) {
            if (!other.tuple.get(i).isEquivalent(tuple.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public List<? extends QueryTreeNode> getChildren() {
        return tuple;
    }

    @Override
    public QueryTreeNode getChild(int index) {
        return tuple.get(index);
    }

    @Override
    public void setChild(int index, QueryTreeNode newValue) {
        tuple.set(index, (ValueNode) newValue);
    }

    @Override
    public ValueNode bindExpression(FromList fromList,
                                    SubqueryList subqueryList,
                                    List<AggregateNode> aggregateVector) throws StandardException {
        List<ValueNode> newTuple = new ArrayList<>();
        for (ValueNode element : tuple) {
            newTuple.add(element.bindExpression(fromList, subqueryList, aggregateVector));
        }
        tuple = newTuple;
        return this;
    }

    public int size() {
        return tuple.size();
    }

    public ValueNode get(int i) {
        return tuple.get(i);
    }

    public void addValueNode(ValueNode value) {
        tuple.add(value);
    }

    @Override
    public JBitSet getTablesReferenced() throws StandardException {
        JBitSet tableNumbers = new JBitSet(size());
        for (ValueNode vn : tuple) {
            tableNumbers.or(vn.getTablesReferenced());
        }
        return tableNumbers;
    }
}
