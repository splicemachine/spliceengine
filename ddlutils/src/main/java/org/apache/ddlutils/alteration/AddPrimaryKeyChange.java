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

import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;

/**
 * Represents the addition of a primary key to a table which does not have one.
 *
 * @version $Revision: $
 */
public class AddPrimaryKeyChange extends TableChangeImplBase {
    /**
     * The names of the columns making up the primary key.
     */
    private String[] _primaryKeyColumns;

    /**
     * Creates a new change object.
     *
     * @param table             The name of the table to add the primary key to
     * @param primaryKeyColumns The names of the columns making up the primary key
     */
    public AddPrimaryKeyChange(Table table, String[] primaryKeyColumns) {
        super(table);
        if (primaryKeyColumns == null) {
            _primaryKeyColumns = new String[0];
        } else {
            _primaryKeyColumns = new String[primaryKeyColumns.length];

            System.arraycopy(primaryKeyColumns, 0, _primaryKeyColumns, 0, primaryKeyColumns.length);
        }
    }

    /**
     * Returns the primary key column names making up the new primary key.
     *
     * @return The primary key column names
     */
    public String[] getPrimaryKeyColumns() {
        String[] result = new String[_primaryKeyColumns.length];

        System.arraycopy(_primaryKeyColumns, 0, result, 0, _primaryKeyColumns.length);
        return result;
    }

    /**
     * {@inheritDoc}
     */
    public void apply(Database model, boolean caseSensitive) {
        Table table = findChangedTable(model, caseSensitive);

        for (int idx = 0; idx < _primaryKeyColumns.length; idx++) {
            Column column = table.findColumn(_primaryKeyColumns[idx], caseSensitive);

            column.setPrimaryKey(true);
        }
    }
}
