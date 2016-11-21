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
 * Base class for changes to columns.
 *
 * @version $Revision: $
 */
public abstract class ColumnChangeImplBase extends TableChangeImplBase
    implements ColumnChange {
    /**
     * The column's name.
     */
    private String _columnName;

    /**
     * Creates a new change object.
     *
     * @param table      The name of the table to remove the column from
     * @param columnName The column's name
     */
    public ColumnChangeImplBase(Table table, String columnName) {
        super(table);
        _columnName = columnName;
    }

    /**
     * {@inheritDoc}
     */
    public String getChangedColumn() {
        return _columnName;
    }

    /**
     * {@inheritDoc}
     */
    public Column findChangedColumn(Database model, boolean caseSensitive) {
        Table table = findChangedTable(model, caseSensitive);

        return table == null ? null : table.findColumn(_columnName, caseSensitive);
    }
}
