/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.db.iapi.sql.dictionary.fk;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.StatementType;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.utils.Pair;

import java.util.*;

public class DictionaryGraphBuilder implements GraphBuilder {

    private final TableDescriptor referencingTableDescriptor;
    private final DataDictionary dd;
    private final ConsInfo newConstraintInfo;
    private final String newConstraintName;
    private final UUID schemaId;
    private final String tableName;
    private final String schemaName;

    static class Edge {
        final String from;
        final EdgeNode.Type type;
        final String to;

        Edge(String from, EdgeNode.Type type, String to) {
            this.from = from;
            this.type = type;
            this.to = to;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Edge edge = (Edge) o;
            return Objects.equals(from, edge.from) &&
                    type == edge.type &&
                    Objects.equals(to, edge.to);
        }

        @Override
        public int hashCode() {
            return Objects.hash(from, type, to);
        }
    }

    private EdgeNode.Type toEdgeNodeType(int action) {
        switch (action) {
            case StatementType.RA_CASCADE:
                return EdgeNode.Type.C;
            case StatementType.RA_NOACTION:
                return EdgeNode.Type.NA;
            case StatementType.RA_RESTRICT:
                return EdgeNode.Type.R;
            case StatementType.RA_SETNULL:
                return EdgeNode.Type.SN;
        }
        throw new IllegalArgumentException("unexpected action type: " + action);
    }

    public DictionaryGraphBuilder(DataDictionary dd,
                                  TableDescriptor referencingTableDescriptor,
                                  String newConstraintName,
                                  ConsInfo newConstraintInfo, String schemaName, UUID schemaId, String tableName) {
        this.dd = dd;
        this.referencingTableDescriptor = referencingTableDescriptor;
        this.newConstraintInfo = newConstraintInfo;
        this.newConstraintName = newConstraintName;
        this.schemaName = schemaName;
        this.schemaId = schemaId;
        this.tableName = tableName;
    }

    List<Pair<TableDescriptor, EdgeNode.Type>> getParents(TableDescriptor tableDescriptor) throws StandardException {
        List<Pair<TableDescriptor, EdgeNode.Type>> result = new ArrayList<>();
        ConstraintDescriptorList constraintDescriptorList = dd.getConstraintDescriptors(tableDescriptor);
        for (ConstraintDescriptor constraintDescriptor : constraintDescriptorList) {
            if (!(constraintDescriptor instanceof ForeignKeyConstraintDescriptor)) { //look for foreign keys only
                continue;
            }
            ForeignKeyConstraintDescriptor foreignKeyConstraintDescriptor = (ForeignKeyConstraintDescriptor) constraintDescriptor;

            // take care of cases where the FK is self-referencing, for now ignore.
            if (foreignKeyConstraintDescriptor.isSelfReferencingFK()) {
                continue;
            }

            ReferencedKeyConstraintDescriptor referencedConstraint = foreignKeyConstraintDescriptor.getReferencedConstraint();
            TableDescriptor referencedConstraintTableDescriptor = referencedConstraint.getTableDescriptor();

            result.add(new Pair<>(referencedConstraintTableDescriptor, toEdgeNodeType(foreignKeyConstraintDescriptor.getRaDeleteRule())));
        }
        return result;
    }

    private ConstraintDescriptorList retrieveFromCache(UUID constraintId) throws StandardException {
        ConstraintDescriptorList result = dd.getDataDictionaryCache().constraintDescriptorListCacheFind(constraintId);
        if(result == null) {
            result = dd.getForeignKeys(constraintId);
            dd.getDataDictionaryCache().constraintDescriptorListCacheAdd(constraintId, result);
        }
        return result;
    }

    List<Pair<TableDescriptor, EdgeNode.Type>> getChildren(TableDescriptor tableDescriptor) throws StandardException {
        List<Pair<TableDescriptor, EdgeNode.Type>> result = new ArrayList<>();
        ConstraintDescriptorList constraintDescriptorList = dd.getConstraintDescriptors(tableDescriptor);
        for (ConstraintDescriptor cd : constraintDescriptorList) {
            if ((cd instanceof ReferencedKeyConstraintDescriptor)) {
                ConstraintDescriptorList fkcdl = retrieveFromCache(cd.getUUID());
                int size = fkcdl.size();
                if (size == 0) {
                    continue;
                }
                for (int inner = 0; inner < size; inner++) {
                    ForeignKeyConstraintDescriptor fkcd = (ForeignKeyConstraintDescriptor) fkcdl.elementAt(inner);
                    // take care of cases where the FK is self-referencing, for now ignore.
                    if (fkcd.isSelfReferencingFK()) {
                        continue;
                    }
                    TableDescriptor fktd = fkcd.getTableDescriptor();
                    result.add(new Pair<>(fktd, toEdgeNodeType(fkcd.getRaDeleteRule())));
                }
            }
        }
        return result;
    }

    @Override
    public Graph generateGraph() throws StandardException {
        Set<Edge> edges = new HashSet<>();
        Set<String> tableNames = new HashSet<>();

        Queue<TableDescriptor> descriptors = new LinkedList();
        if(referencingTableDescriptor != null) { // in CREATE TABLE ... REFERENCES, the referencing table is NULL since it not persisted yet.
            descriptors.add(referencingTableDescriptor);
        }

        TableDescriptor referencedTableDescriptor = newConstraintInfo.getReferencedTableDescriptor(dd);

        if(referencedTableDescriptor == null) { // self-referencing table in DDL
            assert referencingTableDescriptor == null;
            return new Graph(tableNames, newConstraintName); // empty graph.
        }

        descriptors.add(referencedTableDescriptor);



        Set<UUID> visited = new HashSet<>();

        while(!descriptors.isEmpty()) {
            TableDescriptor descriptor = descriptors.remove();
            visited.add(descriptor.getUUID());
            String tableName = descriptor.getSchemaName() + "." + descriptor.getName();
            for(Pair<TableDescriptor, EdgeNode.Type> child : getChildren(descriptor)) {
                String childName = child.getFirst().getSchemaName() + "." + child.getFirst().getName();
                tableNames.add(childName);
                edges.add(new Edge(tableName, child.getSecond(), childName));
                if(!visited.contains(child.getFirst().getUUID())) {
                    descriptors.add(child.getFirst());
                }
            }
            for(Pair<TableDescriptor, EdgeNode.Type> parent : getParents(descriptor)) {

                String parentName = parent.getFirst().getSchemaName() + "." + parent.getFirst().getName();
                tableNames.add(parentName);
                edges.add(new Edge(parentName, parent.getSecond(), tableName));
                if(!visited.contains(parent.getFirst().getUUID())) {
                    descriptors.add(parent.getFirst());
                }
            }
        }

        // finally add the newly added FK (if it is not self-referencing)
        String from = schemaName + "." + tableName;
        String to = referencedTableDescriptor.getSchemaName() + "." + referencedTableDescriptor.getName();
        if(!isSelfReferencing()) {
            int newFKDeletionActionRule = newConstraintInfo.getReferentialActionDeleteRule();
            edges.add(new Edge(to, toEdgeNodeType(newFKDeletionActionRule), from));
        }
        tableNames.add(from);
        tableNames.add(to);

        Graph result = new Graph(tableNames, newConstraintName);
        for(Edge edge : edges) {
            result.addEdge(edge.to, edge.from, edge.type);
        }
        return result;
    }

    private boolean isSelfReferencing() throws StandardException {
        TableDescriptor referencedTableDescriptor = newConstraintInfo.getReferencedTableDescriptor(dd);
        if(referencingTableDescriptor != null) {
            return referencedTableDescriptor.getUUID().equals(referencingTableDescriptor.getUUID());
        } else {
            return referencedTableDescriptor.getSchemaDescriptor().getUUID().equals(schemaId) && referencedTableDescriptor.getName().equals(tableName);
        }
    }
}
