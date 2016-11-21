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

import org.apache.ddlutils.model.CloneHelper;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;

/**
 * Represents the addition of a table to a model. Note that this change does not include foreign keys
 * originating from the new table.
 *
 * @version $Revision: $
 */
public class AddTableChange implements ModelChange {
    /**
     * The new table.
     */
    private Table _newTable;

    /**
     * Creates a new change object.
     *
     * @param newTable The new table; note that the change object will keep a reference to this table
     *                 which means that the table should not be changed after creating this change object
     */
    public AddTableChange(Table newTable) {
        _newTable = newTable;
    }

    /**
     * Returns the new table. Note that only the columns and table-level constraints are to be used.
     * Any model-level constraints (e.g. foreign keys) shall be ignored as there are different change
     * objects for them.
     *
     * @return The new table
     */
    public Table getNewTable() {
        return _newTable;
    }

    /**
     * {@inheritDoc}
     */
    public void apply(Database database, boolean caseSensitive) {
        Table table = new CloneHelper().clone(_newTable, true, false, database, caseSensitive);

        database.addTableIntoSchema(table.getSchema(), table);
    }
}
