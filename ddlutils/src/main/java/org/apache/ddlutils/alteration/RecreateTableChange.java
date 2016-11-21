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

import java.util.List;

import org.apache.ddlutils.model.CloneHelper;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Schema;
import org.apache.ddlutils.model.Table;

/**
 * Represents the recreation of a table, i.e. creating a temporary table using the target table definition,
 * copying the data from the original table into this temporary table, dropping the original table and
 * finally renaming the temporary table to the final table. This change is only created by the model
 * comparator if the table definition change predicate flags a table change as unsupported.
 *
 * @version $Revision: $
 */
public class RecreateTableChange extends TableChangeImplBase {
    /**
     * The original table changes, one of which is unsupported by the current platform.
     */
    private List _originalChanges;

    /**
     * Creates a new change object for recreating a table. This change is used to specify that a table needs
     * to be dropped and then re-created (with changes). In the standard model comparison algorithm, it will
     * replace all direct changes to the table's columns (i.e. foreign key and index changes are unaffected).
     *
     * @param targetTable     The table as it should be; note that the change object will keep a reference
     *                        to this table which means that it should not be changed after creating this
     *                        change object
     * @param originalChanges The original changes that this change object replaces
     */
    public RecreateTableChange(Table targetTable, List originalChanges) {
        super(targetTable);
        _originalChanges = originalChanges;
    }

    /**
     * Returns the original table changes, one of which is unsupported by the current platform.
     *
     * @return The table changes
     */
    public List getOriginalChanges() {
        return _originalChanges;
    }

    /**
     * {@inheritDoc}
     */
    public void apply(Database database, boolean caseSensitive) {
        // we only need to replace the table in the model, as there can't be a
        // foreign key from or to it when these kind of changes are created
        for (Schema schema : database.getSchemas()) {
            for (Table curTable : schema.getTables()) {

                if ((caseSensitive && curTable.getName().equals(getChangedTable().getName())) ||
                    (!caseSensitive && curTable.getName().equalsIgnoreCase(getChangedTable().getName()))) {
                    schema.replaceTable(curTable, new CloneHelper().clone(_table, true, false, database, caseSensitive));
                    break;
                }
            }
        }
    }
}
