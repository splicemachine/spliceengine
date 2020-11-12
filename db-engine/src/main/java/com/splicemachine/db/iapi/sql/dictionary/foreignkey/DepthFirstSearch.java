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

package com.splicemachine.db.iapi.sql.dictionary.foreignkey;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.utils.Pair;

import java.util.*;

/**
 * Searches a graph depth-first. It also offers some utility functions like getting a path between two points and
 * examining the type of edges between vertices during the DFS visit.
 */
public class DepthFirstSearch {
    private final Graph  graph;
    private final String newConstraintName;
    int[]                dfsParent;
    boolean[]            processed;
    boolean[]            discovered;
    EdgeClassification[] edgeTypes;
    Deque<Integer>       stack;
    int[]                entryStep;
    int[]                exitStep;
    int                  stepUnit;
    ArrayList[]          parents;
    boolean              finished;
    List<Integer>        path = new ArrayList<>();
    private boolean      trackParents;

    public DepthFirstSearch(Graph g, String newConstraintName) {
        this.graph = g;
        dfsParent = new int[g.getVertexCount()];
        processed = new boolean[g.getVertexCount()];
        discovered = new boolean[g.getVertexCount()];
        edgeTypes = new EdgeClassification[g.getVertexCount()];
        entryStep = new int[g.getVertexCount()];
        exitStep = new int[g.getVertexCount()];
        init();
        stack = new ArrayDeque<Integer>();
        parents = new ArrayList[g.getVertexCount()];
        this.newConstraintName = newConstraintName;
        finished = false;
    }

    void init() {
        Arrays.fill(dfsParent, -1);
        Arrays.fill(discovered, false);
        Arrays.fill(processed, false);
        Arrays.fill(edgeTypes, EdgeClassification.TREE);
        Arrays.fill(entryStep, 0);
        Arrays.fill(exitStep, 0);
    }

    EdgeClassification edgeClassification(int x, int y) {
        if (dfsParent[y] == x) return EdgeClassification.TREE;
        if (discovered[y] && !processed[y]) return EdgeClassification.BACK;
        if (processed[y] && (entryStep[y] > entryStep[x])) return EdgeClassification.FORWARD;
        if (processed[y] && (entryStep[y] < entryStep[x])) return EdgeClassification.CROSS;
        throw new IllegalArgumentException("unknown edge class between " + x + " and " + y);
    }

    /**
     * initiates depth-first search starting from vertex `v`. This method is recursive, note that the
     * starting vertex determines the relationships between the rest of the nodes in their DFS order
     *
     * @param v the starting node
     *
     * @throws StandardException if an edge of unknown classification is found.
     */
    public void run(int v) throws StandardException {
        discovered[v] = true;
        stepUnit += 1;
        entryStep[v] = stepUnit;
        if (finished) return;
        processVertexEarly(v);
        EdgeNode edge = graph.getEdge(v);
        int y;
        while (edge != null) {
            y = edge.to;
            if (trackParents) {
                if (parents[y] == null) {
                    parents[y] = new ArrayList<Pair<Integer, EdgeNode.Type>>();
                }
                parents[y].add(new Pair<>(v, edge.type));
            }
            if (!discovered[y]) {
                dfsParent[y] = v;
                processEdge(v, y);
                run(y);
            } else {
                processEdge(v, y);
            }
            if (finished) return;
            edge = edge.next;
        }
        processVertexLate(v);
        exitStep[v] = stepUnit;
        processed[v] = true;
    }

    private void processEdge(int x, int y) throws StandardException {
        if (edgeClassification(x, y) == EdgeClassification.BACK) {
            finished = true;
        }
    }

    private void processVertexLate(int v) {
        stack.push(v);
    }

    private void processVertexEarly(int v) {
    }

    public Deque<Integer> topologicalSort() throws StandardException {
        trackParents = true;
        for (int i = 0; i < discovered.length; ++i) {
            if (!discovered[i]) {
                run(i);
                finished = false;
            }
        }

        return stack;
    }

    public List<Integer> getPath(int s, int e) {
        path = new ArrayList<>();
        getPathInternal(s, e);
        return path;
    }

    private void getPathInternal(int s, int e) {
        if (e == -1) {
            return;
        } else if (s == e) {
            path.add(s);
        } else {
            getPathInternal(s, dfsParent[e]);
            path.add(e);
        }
    }

    public ArrayList[] getParents() {
        return parents;
    }

    enum EdgeClassification {TREE, BACK, FORWARD, CROSS}
}
