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

import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;

/**
 * Represents a change to a table or sub-element of a table (e.g. a column).
 *
 * @version $Revision: $
 */
public interface TableChange extends ModelChange {

    /**
     * Returns the affected table from the original model.
     *
     * @return The name of the affected table
     */
    Table getChangedTable();

    /**
     * Finds the table object corresponding to the changed table in the given database model.
     *
     * @param model         The database model
     * @param caseSensitive Whether identifiers are case sensitive
     * @return The table object or <code>null</code> if it could not be found
     */
    Table findChangedTable(Database model, boolean caseSensitive);
}
