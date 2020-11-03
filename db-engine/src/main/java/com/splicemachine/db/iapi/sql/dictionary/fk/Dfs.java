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
import javafx.util.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;

public class Dfs {
    private final Graph graph;
    private final String newConstraintName;
    int[] dfsParent;
    boolean[] processed;
    boolean[] discovered;
    private boolean trackParents;

    enum EdgeClassification {TREE, BACK, FORWARD, CROSS};
    EdgeClassification[] edgeTypes;
    Stack<Integer> stack;
    int[] entryTime;
    int[] exitTime;
    int time;
    ArrayList[] parents;


    public Dfs(Graph g, String newConstraintName) {
        this.graph = g;
        dfsParent = new int[g.getVertexCount()];
        processed = new boolean[g.getVertexCount()];
        discovered = new boolean[g.getVertexCount()];
        edgeTypes = new EdgeClassification[g.getVertexCount()];
        entryTime = new int[g.getVertexCount()];
        exitTime = new int[g.getVertexCount()];
        init();
        stack = new Stack<Integer>();
        parents = new ArrayList[g.getVertexCount()];
        this.newConstraintName = newConstraintName;
    }

    void init() {
        Arrays.fill(dfsParent, -1);
        Arrays.fill(discovered, false);
        Arrays.fill(processed, false);
        Arrays.fill(edgeTypes, EdgeClassification.TREE);
        Arrays.fill(entryTime, 0);
        Arrays.fill(exitTime, 0);
    }

    EdgeClassification edgeClassification(int x, int y) {
        if(dfsParent[y] == x) return EdgeClassification.TREE;
        if(discovered[y] && !processed[y]) return EdgeClassification.BACK;
        if(processed[y] && (entryTime[y] > entryTime[x])) return EdgeClassification.FORWARD;
        if(processed[y] && (entryTime[y] < entryTime[x])) return EdgeClassification.CROSS;
        throw new IllegalArgumentException("unknown edge class between " + x + " and " + y);
    }

    public void run(int v) throws StandardException {
        discovered[v] = true;
        time += 1;
        entryTime[v] = time;
        processVertexEarly(v);
        EdgeNode edge = graph.getEdge(v);
        int y;
        while (edge != null) {
            y = edge.y;
            if(trackParents) {
                if(parents[y] == null) {
                    parents[y] = new ArrayList<Pair<Integer, EdgeNode.Type>>();
                }
                parents[y].add(new Pair<>(v, edge.type));
            }
            if(!discovered[y]) {
                dfsParent[y] = v;
                processEdge(v, y);
                run(y);
            } else {
                processEdge(v, y);
            }
            edge = edge.next;
        }
        processVertexLate(v);
        exitTime[v] = time;
        processed[v] = true;
    }

    private void processEdge(int x, int y) throws StandardException {
        if(edgeClassification(x, y) == EdgeClassification.BACK) {
            StringBuilder sb = new StringBuilder();
            printPath(y, x, sb);
            throw StandardException.newException(SQLState.LANG_DELETE_RULE_VIOLATION,
                                                 newConstraintName,
                                                 String.format("adding this foreign key leads to a cycle from '%s' to '%s' following this path '%s'",
                                                               graph.getName(x), graph.getName(y), sb.toString()));
        }
    }

    private void processVertexLate(int v) {
        stack.push(v);
    }

    private void processVertexEarly(int v) {}

    private void printPath(int s, int e, StringBuilder sb) {
        if(s == e || e == -1) {
            sb.append(graph.getName(s));
        } else {
            printPath(s, dfsParent[e], sb);
            sb.append(" ").append(graph.getName(e));
        }
    }

    public List<Integer> topologicalSort() throws StandardException {
        trackParents = true;
        for(int i=0; i < discovered.length; ++i) {
            if(!discovered[i]) {
                run(i);
            }
        }

        return stack;
    }

    public ArrayList[] getParents() {
        return parents;
    }
}
