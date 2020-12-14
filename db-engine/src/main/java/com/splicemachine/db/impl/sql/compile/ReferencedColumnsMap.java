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
 */

package com.splicemachine.db.impl.sql.compile;

import static com.cedarsoftware.util.DeepEquals.deepEquals;
import com.splicemachine.db.iapi.error.StandardException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import splice.com.google.common.collect.Sets;
import java.util.*;

import static com.splicemachine.db.shared.common.reference.SQLState.LANG_INTERNAL_ERROR;

/**
 * A map from Table Number to a set of column numbers from that table.
 * Used primarily to track which columns in a table are referenced in a predicate.
 * Throws an error if an attempt is made to track a negative table number
 * or column number less than 1.
 */

public final class ReferencedColumnsMap {

    // Key:   Table Number
    // Value: The column numbers of the ColumnReference nodes found for that table
    private Map<Integer, Set<Integer>> tableColumns;

    public ReferencedColumnsMap() {
        tableColumns = new HashMap<>();
    }

    public ReferencedColumnsMap(ReferencedColumnsMap refColsToCopy) {
        Map<Integer, Set<Integer>> mapToCopy = refColsToCopy.getTableColumns();
        tableColumns = new HashMap<>(mapToCopy.size());

        Iterator mapIterator = mapToCopy.entrySet().iterator();
        // Make more than just a shallow copy because sharing the set of column numbers
        // would mean when we call method 'add' on one ReferencedColumnsMap, the copy
        // would get updated too, which we don't want.
        while (mapIterator.hasNext()) {
            Map.Entry<Integer, Set<Integer>> mapElement = (Map.Entry<Integer, Set<Integer>>) mapIterator.next();
            Set<Integer> valueToCopy = mapElement.getValue();
            Set<Integer> newValue = new HashSet<Integer>(valueToCopy);
            Integer key = mapElement.getKey();
            tableColumns.put(key, newValue);
        }
    }

    public int getNumReferencedTables() {
        return tableColumns.size();
    }

    // Find the referenced column numbers for a given table number.
    public Set<Integer> get(int tableNumber) {
        return tableColumns.get(tableNumber);
    }

    // Add to the map a reference to a columnNumber from a given tableNumber.
    public ReferencedColumnsMap add(int tableNumber, int columnNumber) throws StandardException {

        if (tableNumber < 0)
            throw StandardException.newException(LANG_INTERNAL_ERROR,
                    "Attempt to add negative table number to ReferencedColumnsMap.");
        if (columnNumber < 1)
            throw StandardException.newException(LANG_INTERNAL_ERROR,
                    "Attempt to add non-positive column number to ReferencedColumnsMap.");

        Set<Integer> foundSet = tableColumns.get(tableNumber);
        try {
            if (foundSet == null) {
                // Keep the initial size small to save memory.
                foundSet = new HashSet<>(1);
                foundSet.add(columnNumber);
                tableColumns.put(tableNumber, foundSet);
            }
            foundSet.add(columnNumber);
        }
        catch (Exception e) {
            throw StandardException.newException(LANG_INTERNAL_ERROR, e,
               "Cannot add to referenced column map.");
        }
        return this;
    }

    // Test if this map shares any columns with a given table number and set of column numbers.
    private boolean intersects(int tableNumber, Set<Integer> columnNumbers) throws StandardException {
        Set<Integer> foundSet = tableColumns.get(tableNumber);
        try {
            if (foundSet == null || columnNumbers == null) {
                return false;
            }
            Sets.SetView<Integer> resultSet = Sets.intersection(foundSet, columnNumbers);
            return !resultSet.isEmpty();
        }
        catch (Exception e) {
            throw StandardException.newException(LANG_INTERNAL_ERROR, e,
               "Cannot do intersection test on referenced columns map.");
        }
    }

    // Test if this map shares any columns with another map for a given table number.
    public boolean intersects(int tableNumber, ReferencedColumnsMap otherMap) throws StandardException {
        Set<Integer> otherFoundSet = otherMap.get(tableNumber);
        return intersects(tableNumber, otherFoundSet);
    }

    @SuppressFBWarnings(value = "EQ_SELF_USE_OBJECT",justification = "intentional")
    public boolean equals(ReferencedColumnsMap refColsToCompare) {
        Map<Integer, Set<Integer>> otherTableColumns = refColsToCompare.getTableColumns();
        if (tableColumns == otherTableColumns)
            return true;
        if (tableColumns == null || otherTableColumns == null)
            return false;
        else if (tableColumns.isEmpty())
            return otherTableColumns.isEmpty();
        else if (otherTableColumns.isEmpty())
            return false;

        return deepEquals(tableColumns, otherTableColumns);
    }

    public int hashCode() {
        return tableColumns.hashCode();
    }

    // For internal use only.
    private Map<Integer, Set<Integer>>  getTableColumns() {
        return tableColumns;
    }

}
