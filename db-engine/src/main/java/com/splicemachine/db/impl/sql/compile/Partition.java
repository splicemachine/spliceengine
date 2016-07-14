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

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;

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

    @Override
    public int hashCode() {
        int result = 31;
        for (int i=0; i<size(); i++) {
            result = 31 * result +
                (this.getGroupByColumn(i).getColumnExpression().getColumnName() == null ? 0 :
                    this.getGroupByColumn(i).getColumnExpression().getColumnName().hashCode());
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        try {
            return isEquivalent((Partition) o);
        } catch (StandardException e) {
            // ignore
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        for (int i=0; i<size(); ++i) {
            GroupByColumn col = getGroupByColumn(i);
            buf.append(col.getColumnName()).append("(").append(col.getColumnPosition()).append("),");
        }
        if (buf.length() > 0) { buf.setLength(buf.length()-1); }
        return buf.toString();
    }

    @Override
    public String toHTMLString() {
        StringBuilder buf = new StringBuilder();
        for (int i=0; i<size(); ++i) {
            GroupByColumn col = getGroupByColumn(i);
            buf.append(col.getColumnName()).append('(');
            buf.append(col.getColumnPosition()).append("),");
        }
        if (buf.length() > 0) { buf.setLength(buf.length()-1); }
        return buf.toString();
    }
}
