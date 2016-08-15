/*
 * ddlUtils is a subproject of the Apache DB project, and is licensed under
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
 */

package org.apache.ddlutils.alteration;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;

/**
 * Represents the change of the order of the columns of a table.
 *
 * @version $Revision: $
 */
public class ColumnOrderChange extends TableChangeImplBase {
    /**
     * The map containing the new positions keyed by the source columns.
     */
    private Map _newPositions;

    /**
     * Creates a new change object.
     *
     * @param table        The able whose primary key is to be changed
     * @param newPositions The map containing the new positions keyed by the source column names
     */
    public ColumnOrderChange(Table table, Map newPositions) {
        super(table);
        _newPositions = newPositions;
    }

    /**
     * Returns the new position of the given source column.
     *
     * @param sourceColumnName The column's name
     * @param caseSensitive    Whether case of the column name matters
     * @return The new position or -1 if no position is marked for the column
     */
    public int getNewPosition(String sourceColumnName, boolean caseSensitive) {
        Integer newPos = null;

        if (caseSensitive) {
            newPos = (Integer) _newPositions.get(sourceColumnName);
        } else {
            for (Iterator it = _newPositions.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry entry = (Map.Entry) it.next();

                if (sourceColumnName.equalsIgnoreCase((String) entry.getKey())) {
                    newPos = (Integer) entry.getValue();
                    break;
                }
            }
        }

        return newPos == null ? -1 : newPos.intValue();
    }

    /**
     * {@inheritDoc}
     */
    public void apply(Database database, boolean caseSensitive) {
        Table table = findChangedTable(database, caseSensitive);
        ArrayList newColumns = new ArrayList();

        for (int idx = 0; idx < table.getColumnCount(); idx++) {
            newColumns.add(table.getColumn(idx));
        }
        for (int idx = 0; idx < table.getColumnCount(); idx++) {
            Column column = table.getColumn(idx);
            int newPos = getNewPosition(column.getName(), caseSensitive);

            if (newPos >= 0) {
                newColumns.set(newPos, column);
            }
        }
        table.removeAllColumns();
        table.addColumns(newColumns);
    }
}
