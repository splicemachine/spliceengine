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

import java.text.Collator;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.sun.istack.internal.NotNull;

/**
 * Represents a schema in database model, ie. the container of tables in the database. It also
 * contains the corresponding dyna classes for creating dyna beans for the
 * objects stored in the tables.
 */
public class Schema implements Comparable<Schema> {

    private final String schemaId;
    private final String schemaName;
    private final String authorizationId;

    private List<Table> tables = new ArrayList<>();

    public Schema(String schemaName, String schemaId, String authorizationId) {
        this.authorizationId = authorizationId;
        this.schemaId = schemaId;
        this.schemaName = schemaName;
    }

    public String getAuthorizationId() {
        return authorizationId;
    }

    public String getSchemaId() {
        return schemaId;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public Schema addTable(Table table) {
        this.tables.add(table);
        return this;
    }

    public int getTableCount() {
        return tables.size();
    }

    public List<Table> getTables() {
        return tables;
    }

    public List<Table> getTablesOfType(Predicate<TableType> typePredicate) {
        return tables.stream().filter(t -> typePredicate.test(t.getType())).collect(Collectors.toList());
    }

    public List<Table> getTablesNotOfType(Predicate<TableType> typePredicate) {
        return tables.stream().filter(t -> typePredicate.negate().test(t.getType())).collect(Collectors.toList());
    }

    public List<Table> getTablesInReverseOrder() {
        return Lists.reverse(tables);
    }

    @Override
    public int hashCode() {
        int result = schemaId.hashCode();
        result = 31 * result + schemaName.hashCode();
        result = 31 * result + authorizationId.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Schema schema = (Schema) o;

        if (!schemaId.equals(schema.schemaId)) return false;
        if (!schemaName.equals(schema.schemaName)) return false;
        return authorizationId.equals(schema.authorizationId);

    }

    @Override
    public String toString() {
        return "Schema{" +
            "schemaName='" + schemaName + '\'' +
            ", schemaId='" + schemaId + '\'' +
            ", authorizationId='" + authorizationId + '\'' +
            '}';
    }

    @Override
    public int compareTo(@NotNull Schema o) {
        final Collator collator = Collator.getInstance();
        return collator.compare(schemaName.toUpperCase(), o.schemaName.toUpperCase());
    }

    public void replaceTable(Table curTable, Table replacementTable) {
        for (int i = 0; i < tables.size(); i++) {
            if (tables.get(i).equals(curTable)) {
                tables.remove(i);
                tables.add(i, replacementTable);
                break;
            }
        }
    }

    public void removeTable(Table table) {
        tables.remove(table);
    }
}
