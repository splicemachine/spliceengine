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

import java.util.*;

public class Graph {
    List<EdgeNode> edgeNodes;
    Map<String, Integer> vertexIndex;
    int[][] parents;

    public Graph(Set<String> vertices) {
        edgeNodes = new ArrayList<>(vertices.size());
        vertexIndex = new HashMap<>(vertices.size());
        int i = 0;
        for (String vertex : vertices) {
            edgeNodes.add(null);
            vertexIndex.put(vertex, i++);
        }
        parents = new int[vertices.size()][];
    }

    void addEdge(String from, String to, EdgeNode.Type type) {
        EdgeNode edgeNode = new EdgeNode(vertexIndex.get(to), type);
        edgeNode.next = edgeNodes.get(vertexIndex.get(from));
        edgeNodes.set(vertexIndex.get(from), edgeNode);
    }

    public String getName(int index) {
        for (Map.Entry<String, Integer> entry : vertexIndex.entrySet()) {
            if (entry.getValue() == index) {
                return entry.getKey();
            }
        }
        throw new IllegalArgumentException("could not find name for item at the index: " + index);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("digraph {").append("\n");
        int i = 0;
        for (EdgeNode edge : edgeNodes) {
            EdgeNode next = edge;
            while (next != null) {
                sb.append("\t").append(getName(i)).append(" -> ").append(getName(next.y)).append("[label=\"").append(next.type.toString()).append("\"];").append("\n");
                next = next.next;
            }
            i++;
        }
        sb.append("}").append("\n");
        return sb.toString();
    }

    public EdgeNode getEdge(int v) {
        return edgeNodes.get(v);
    }

    public int getVertexCount() {
        return vertexIndex.size();

    }
}
