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

package org.apache.ddlutils.model;

import java.util.ArrayList;

import org.apache.ddlutils.util.StringUtilsExt;

/**
 * Base class for indexes.
 *
 * @version $Revision: $
 */
public abstract class IndexImplBase implements Index {
    /**
     * The name of the index.
     */
    protected String _name;
    /**
     * The columns making up the index.
     */
    protected ArrayList _columns = new ArrayList();

    /**
     * {@inheritDoc}
     */
    public String getName() {
        return _name;
    }

    /**
     * {@inheritDoc}
     */
    public void setName(String name) {
        _name = name;
    }

    /**
     * {@inheritDoc}
     */
    public int getColumnCount() {
        return _columns.size();
    }

    /**
     * {@inheritDoc}
     */
    public IndexColumn getColumn(int idx) {
        return (IndexColumn) _columns.get(idx);
    }

    /**
     * {@inheritDoc}
     */
    public IndexColumn[] getColumns() {
        return (IndexColumn[]) _columns.toArray(new IndexColumn[_columns.size()]);
    }

    /**
     * {@inheritDoc}
     */
    public boolean hasColumn(Column column) {
        for (int idx = 0; idx < _columns.size(); idx++) {
            IndexColumn curColumn = getColumn(idx);

            if (column.equals(curColumn.getColumn())) {
                return true;
            }
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    public boolean hasColumn(String columnName, boolean caseSensitive) {
        for (int idx = 0; idx < _columns.size(); idx++) {
            IndexColumn curColumn = getColumn(idx);

            if (StringUtilsExt.equals(columnName, curColumn.getName(), caseSensitive)) {
                return true;
            }
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    public void addColumn(IndexColumn column) {
        if (column != null) {
            for (int idx = 0; idx < _columns.size(); idx++) {
                IndexColumn curColumn = getColumn(idx);

                if (curColumn.getOrdinalPosition() > column.getOrdinalPosition()) {
                    _columns.add(idx, column);
                    return;
                }
            }
            _columns.add(column);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void removeColumn(IndexColumn column) {
        _columns.remove(column);
    }

    /**
     * {@inheritDoc}
     */
    public void removeColumn(int idx) {
        _columns.remove(idx);
    }
}
