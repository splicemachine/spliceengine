/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.sql;

/**
 * @author Scott Fines
 *         Date: 1/28/15
 */
public abstract class BaseColumn implements ColumnMetaData{
    private final boolean searchable;
    private final ColumnNullability nullable;

    private final int displaySize;
    private final String label;
    private final String name;
    private final String catalog;
    private final String schema;
    private final String table;
    private final SQLType type;
    private final boolean readOnly;

    private final String colClassName;

    protected BaseColumn(boolean searchable,
                      ColumnNullability nullable,
                      int displaySize,
                      String label,
                      String name,
                      String catalog,
                      String schema,
                      String table,
                      SQLType type,
                      boolean readOnly,
                      String colClassName) {
        this.searchable = searchable;
        this.nullable = nullable;
        this.displaySize = displaySize;
        this.label = label;
        this.name = name;
        this.catalog = catalog;
        this.schema = schema;
        this.table = table;
        this.type = type;
        this.readOnly = readOnly;
        this.colClassName = colClassName;
    }

    @Override public String getColumnClassName() { return colClassName; }

    @Override public boolean isSearchable() { return searchable; }
    @Override public ColumnNullability nullability() { return nullable; }
    @Override public int getColumnDisplaySize() { return displaySize; }

    @Override public String getLabel() { return label; }
    @Override public String getName() { return name; }
    @Override public String getSchema() { return schema; }
    @Override public String getTable() { return table; }
    @Override public String getCatalog() { return catalog; }
    @Override public SQLType getType() { return type; }

    @Override public boolean isReadOnly() { return readOnly; }
    @Override public boolean isWritable() { return !readOnly; }
}
