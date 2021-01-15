package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.util.JBitSet;

import java.util.ArrayList;
import java.util.List;

/**
 * A ValueTupleNode represents a tuple of ValueNodes that can be used for example in
 * select * from t where (a,b) = (0,0)
 *
 */
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
    protected boolean isSemanticallyEquivalent(ValueNode o) throws StandardException {
        if (!isSameNodeType(o)) {
            return false;
        }
        ValueTupleNode other = (ValueTupleNode) o;
        if (other.tuple.size() != tuple.size()) {
            return false;
        }

        for (int i = 0; i < tuple.size(); i++) {
            if (!other.tuple.get(i).isSemanticallyEquivalent(tuple.get(i))) {
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        int result = getBaseHashCode();
        for (ValueNode vn : tuple) {
            result = 31 * result + vn.hashCode();
        }
        return result;
    }

    @Override
    public void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);

        for (ValueNode vn : tuple) {
            vn.accept(v, this);
        }
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

    @Override
    public boolean categorize(JBitSet referencedTabs, ReferencedColumnsMap referencedColumns, boolean simplePredsOnly) throws StandardException {
        for (ValueNode vn : tuple) {
            if (!vn.categorize(referencedTabs, referencedColumns, simplePredsOnly)) {
                return false;
            }
        }
        return true;
    }

    public boolean typeComparable(ValueTupleNode other) {
        if (other.tuple.size() != tuple.size()) {
            return false;
        }
        for (int i = 0; i < tuple.size(); i++) {
            if (!tuple.get(i).getTypeServices().comparable(other.tuple.get(i).getTypeServices())) {
                return false;
            }
        }
        return true;
    }

    public boolean containsNullableElement() {
        for (ValueNode vn : tuple) {
            if (vn.getTypeServices().isNullable()) {
                return true;
            }
        }
        return false;
    }
}
