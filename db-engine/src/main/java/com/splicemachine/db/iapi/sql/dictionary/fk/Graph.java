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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;

import java.util.*;

public class Graph {
    List<EdgeNode> edgeNodes;
    Map<String, Integer> vertexIndex;
    int[][] parents;

    Map<Integer, Integer> surrogates;
    int surrogateCounter;
    String newConstraintName;

    public Graph(Set<String> vertices, String newConstraintName) {
        edgeNodes = new ArrayList<>(vertices.size());
        vertexIndex = new HashMap<>(vertices.size());
        int i = 0;
        for (String vertex : vertices) {
            edgeNodes.add(null);
            vertexIndex.put(vertex, i++);
        }
        parents = new int[vertices.size()][];
        surrogateCounter = 0;
        this.newConstraintName = newConstraintName;
        this.surrogates = new HashMap<>();
    }

    void addEdge(String from, String to, EdgeNode.Type type) throws StandardException {
        addEdgeInternal(vertexIndex.get(from), vertexIndex.get(to), type);
    }

    private void addSurrogate(int fromIdx, int toIdx, EdgeNode.Type type) {
        int surrogateIdx = vertexIndex.size();
        String name = getName(toIdx) + "__SURROGATE__" + surrogateCounter++;
        surrogates.put(surrogateIdx, toIdx);
        vertexIndex.put(name, surrogateIdx);
        EdgeNode edgeNode = new EdgeNode(surrogateIdx, type);
        edgeNode.next = edgeNodes.get(fromIdx);
        edgeNodes.set(fromIdx, edgeNode);
        edgeNodes.add(null); // for the surrogate
    }

    void addEdgeInternal(int fromIdx, int toIdx, EdgeNode.Type type) throws StandardException {
        Dfs dfs = new Dfs(this, newConstraintName);
        dfs.run(toIdx);
        List<Integer> p = dfs.getPath(toIdx, fromIdx);
        if(p.size() >= 2 && p.get(0) == toIdx && p.get(p.size() - 1) == fromIdx) {
            breakCycle(p, type, fromIdx, toIdx);
        } else {
            EdgeNode edgeNode = new EdgeNode(toIdx, type);
            edgeNode.next = edgeNodes.get(fromIdx);
            edgeNodes.set(fromIdx, edgeNode);
        }
    }

    private void breakCycle(List<Integer> p, EdgeNode.Type type, int fromIdx, int toIdx) throws StandardException {
        if(type != EdgeNode.Type.C) {
            addSurrogate(fromIdx, toIdx, type);
            return;
        } else {
            for(int i = 1; i < p.size(); i++) { // find first none-C in the cycle and break it up
                EdgeNode.Type edgeType = getEdgeType(p.get(i-1), p.get(i));
                if(edgeType != EdgeNode.Type.C) {
                    addSurrogate(p.get(i-1), p.get(i), edgeType);
                    // remove this edge
                    removeEdge(p.get(i-1), p.get(i));
                    // add the new edge again, but check again for cycles!
                    addEdgeInternal(fromIdx, toIdx, type);
                    return;
                }
            }
        }
        // cycle is unbreakable, bail out
        StringBuilder sb = new StringBuilder();
        sb.append("adding the constraint between ").append(
                getName(p.get(0))).append(" and ").append(getName(p.get(p.size()-1))).append(" would cause the following illegal delete action cascade cycle");
        for(int v : p) {
            sb.append(" ").append(getName(v));
        }
        throw StandardException.newException(SQLState.LANG_DELETE_RULE_VIOLATION,
                                             newConstraintName,
                                             sb.toString());
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

    public EdgeNode.Type getEdgeType(int from, int to) {
        EdgeNode edgeNode = getEdge(from);
        while(true) {
            if(edgeNode == null) {
                throw new IllegalArgumentException("no edge between " + getName(from) + " and " + getName(to));
            }
            if(edgeNode.y == to) {
                return edgeNode.type;
            }
            edgeNode = edgeNode.next;
        }
    }

    public void removeEdge(int from, int to) {
        EdgeNode edgeNode = getEdge(from);
        if(edgeNode == null) {
            throw new IllegalArgumentException("no edge between " + getName(from) + " and " + getName(to));
        }
        if(edgeNode.y == to) {
            edgeNodes.set(from, null);
        } else {
            EdgeNode previous = edgeNode;
            edgeNode = edgeNode.next;
            while(true) {
                if(edgeNode == null) {
                    throw new IllegalArgumentException("no edge between " + getName(from) + " and " + getName(to));
                }
                if(edgeNode.y == to) {
                    previous.next = edgeNode.next;
                    edgeNode.next = null; // bye, GC.
                    return;
                }
                edgeNode = edgeNode.next;
            }
        }
    }

    public boolean isSurrogate(int i) {
        return surrogates.containsKey(i);
    }

    public int getOriginal(int surrogate) {
        assert isSurrogate(surrogate);
        return surrogates.get(surrogate);
    }
}
