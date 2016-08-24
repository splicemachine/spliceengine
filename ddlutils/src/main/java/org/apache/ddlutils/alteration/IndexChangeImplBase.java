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
import java.util.List;

import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Index;
import org.apache.ddlutils.model.Table;

/**
 * The base class for changes affecting indexes.
 *
 * @version $Revision: $
 */
public abstract class IndexChangeImplBase extends TableChangeImplBase
    implements IndexChange {
    /**
     * The names of the columns in the index.
     */
    private List _columnNames = new ArrayList();

    /**
     * Creates a new change object.
     *
     * @param table The name of the changed table
     * @param index The index; note that this change object will not maintain a reference
     */
    public IndexChangeImplBase(Table table, Index index) {
        super(table);
        for (int colIdx = 0; colIdx < index.getColumnCount(); colIdx++) {
            _columnNames.add(index.getColumn(colIdx).getName());
        }
    }

    /**
     * {@inheritDoc}
     */
    public Index findChangedIndex(Database model, boolean caseSensitive) {
        Table table = findChangedTable(model, caseSensitive);

        if (table != null) {
            for (int indexIdx = 0; indexIdx < table.getIndexCount(); indexIdx++) {
                Index curIndex = table.getIndex(indexIdx);

                if (curIndex.getColumnCount() == _columnNames.size()) {
                    for (int colIdx = 0; colIdx < curIndex.getColumnCount(); colIdx++) {
                        String curColName = curIndex.getColumn(colIdx).getName();
                        String expectedColName = (String) _columnNames.get(colIdx);

                        if ((caseSensitive && curColName.equals(expectedColName)) ||
                            (!caseSensitive && curColName.equalsIgnoreCase(expectedColName))) {
                            return curIndex;
                        }
                    }
                }
            }
        }
        return null;
    }
}
