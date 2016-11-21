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
 * Base class for change implementations.
 *
 * @version $Revision: $
 */
public abstract class TableChangeImplBase implements TableChange {
    /**
     * The affected table.
     */
    protected Table _table;

    /**
     * Creates a new change object.
     *
     * @param table The table's name
     */
    public TableChangeImplBase(Table table) {
        _table = table;
    }

    /**
     * {@inheritDoc}
     */
    public Table getChangedTable() {
        return _table;
    }

    /**
     * {@inheritDoc}
     */
    public Table findChangedTable(Database model, boolean caseSensitive) {
        return model.findTable(_table.getSchema(), _table.getName(), caseSensitive);
    }
}
