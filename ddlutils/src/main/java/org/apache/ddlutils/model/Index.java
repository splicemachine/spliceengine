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

import java.io.Serializable;

/**
 * Represents an index definition for a table which may be either unique or non-unique.
 *
 * @version $Revision: 893917 $
 */
public interface Index extends Serializable {
    /**
     * Determines whether this index is unique or not.
     *
     * @return <code>true</code> if the index is an unique one
     */
    boolean isUnique();

    /**
     * Returns the name of the index.
     *
     * @return The name
     */
    String getName();

    /**
     * Sets the name of the index.
     *
     * @param name The name
     */
    void setName(String name);

    /**
     * Returns the number of columns that make up this index.
     *
     * @return The number of index columns
     */
    int getColumnCount();

    /**
     * Returns the indicated column making up this index.
     *
     * @param idx The index of the column
     * @return The column
     */
    IndexColumn getColumn(int idx);

    /**
     * Returns the columns that make up this index.
     *
     * @return The columns
     */
    IndexColumn[] getColumns();

    /**
     * Determines whether this index includes the given column.
     *
     * @param column The column to check for
     * @return <code>true</code> if the column is included in this index
     */
    boolean hasColumn(Column column);

    /**
     * Determines whether this index includes the indicated column.
     *
     * @param columnName    The name of the column to check for
     * @param caseSensitive Whether the case of the column name matters for the check
     * @return <code>true</code> if the column is included in this index
     */
    boolean hasColumn(String columnName, boolean caseSensitive);

    /**
     * Adds a column that makes up this index.
     *
     * @param column The column to add
     */
    void addColumn(IndexColumn column);

    /**
     * Removes the given index column from this index.
     *
     * @param column The column to remove
     */
    void removeColumn(IndexColumn column);

    /**
     * Removes the column at the specified position in this index.
     *
     * @param idx The position of the index column to remove
     */
    void removeColumn(int idx);

    /**
     * Returns a clone of this index object.
     *
     * @return The clone
     */
    Index getClone() throws ModelException;

    /**
     * Compares this index to the given one while ignoring the case of identifiers.
     *
     * @param otherIndex The other index
     * @return <code>true</code> if this index is equal (ignoring case) to the given one
     */
    boolean equalsIgnoreCase(Index otherIndex);

    /**
     * Returns a verbose string representation of this index.
     *
     * @return The string representation
     */
    String toVerboseString();
}
