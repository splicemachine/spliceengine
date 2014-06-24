package org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.error.StandardException;

/**
 * Re purposing GroupByList as Partition.
 *
 * @author Jeff Cunningham
 *         Date: 6/9/14
 */
public class Partition extends GroupByList {

    // not overriding, just re-labeling

    public Partition(GroupByList groupByList) {
        super();
        setContextManager(groupByList.getContextManager());
        setNodeType(groupByList.getNodeType());
        if (groupByList.isRollup()) {
            setRollup();
        }
        for (int i=0; i<groupByList.size(); i++) {
            addGroupByColumn(groupByList.getGroupByColumn(i));
            setContextManager(groupByList.getContextManager());
            // TODO: what else am I forgetting to set here
        }
    }

    public boolean isEquivalent(Partition other) throws StandardException {
        if (this == other) return true;
        if (other == null) return false;
        if (this.isRollup() != other.isRollup()) return false;
        if (this.size() != other.size()) return false;

        for (int i=0; i<size(); i++) {
            if (! this.getGroupByColumn(i).getColumnExpression().isEquivalent(other.getGroupByColumn(i).getColumnExpression()))
                return false;
        }
        return true;
    }

    @Override
    public boolean isRollup() {
        // Window partitions differ from GroupBy in that we don't have rollups
        return false;
    }
}
