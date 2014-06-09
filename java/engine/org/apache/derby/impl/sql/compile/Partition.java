package org.apache.derby.impl.sql.compile;

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
        for (int i=0; i<groupByList.getNumNeedToAddGroupingCols(); i++) {
            addGroupByColumn(groupByList.getGroupByColumn(i));
        }
    }
}
