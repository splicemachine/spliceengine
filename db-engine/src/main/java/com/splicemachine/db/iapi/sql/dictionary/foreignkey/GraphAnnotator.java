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
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.utils.Pair;

import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class GraphAnnotator {
    private final Graph graph;
    private final String newConstraintName;

    class Path {
        public Path(List<Integer> vertices, EdgeNode.Type action) {
            this.vertices = vertices;
            this.action = action;
        }

        public String toString(Graph g) {
            StringBuilder sb = new StringBuilder();
            for(int v : removeSurrogates(vertices)) {
                sb.append(g.getName(v)).append(" ");
            }
            sb.append("(delete action: ").append(action).append(")");
            return sb.toString();
        }

        List<Integer> vertices;
        EdgeNode.Type action;

        private List<Integer> removeSurrogates(List<Integer> in) {
            return in.stream().map((vertex) -> {
                if (graph.isSurrogate(vertex)) {
                    return graph.getOriginal(vertex);
                } else {
                    return vertex;
                }
            }).collect(Collectors.toList());
        }

        public boolean intersects(Path needle) {
            List<Integer> originalPathVertices = removeSurrogates(vertices);
            List<Integer> originalNeedleVertices = removeSurrogates(needle.vertices);


            return originalPathVertices.stream()
                    .distinct()
                    .filter(originalNeedleVertices::contains)
                    .collect(Collectors.toSet()).size() > 0;
        }
    }

    static class Annotation {
        List<Path> paths;
        public Annotation() {
            paths = new ArrayList<>();
        }
    }

    Annotation[] annotations;

    public GraphAnnotator(String newConstraintName, Graph graph) {
        this.graph = graph;
        annotations = new Annotation[graph.getVertexCount()];
        for(int i = 0; i < annotations.length; ++i) {
            annotations[i] = new Annotation();
        }
        this.newConstraintName = newConstraintName;
    }

    public void annotate() throws StandardException {
        DepthFirstSearch depthFirstSearch = new DepthFirstSearch(graph, newConstraintName);
        Deque<Integer> schedule = depthFirstSearch.topologicalSort();

        List<Pair<Integer, EdgeNode.Type>>[] parents = depthFirstSearch.getParents();
        Iterator<Integer> it = schedule.descendingIterator();
        while(it.hasNext()) {
            int v = it.next();
            if(parents[v] == null) {
                continue; // no parents;
            }
            List<Path> childPaths = annotations[v].paths;
            for(Pair<Integer, EdgeNode.Type> p : parents[v]) {
                if(childPaths.isEmpty()) {
                    ArrayList<Integer> vertices = new ArrayList<>();
                    vertices.add(v);
                    annotations[p.getFirst()].paths.add(new Path(vertices, p.getSecond()));
                } else {
                    for(Path childPath : childPaths) {
                        if(childPath.action != EdgeNode.Type.Cascade) {
                            ArrayList<Integer> vertices = new ArrayList<>();
                            vertices.add(v);
                            annotations[p.getFirst()].paths.add(new Path(vertices, p.getSecond()));
                        } else {
                            List<Integer> vertices = new ArrayList<>(childPath.vertices);
                            vertices.add(v);
                            Path parentPath = new Path(vertices, p.getSecond());
                            annotations[p.getFirst()].paths.add(parentPath);
                        }
                    }
                }
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for(int i=0; i < annotations.length; ++i) {
            sb.append(graph.getName(i)).append(" : \n");
            for(Path path : annotations[i].paths) {
                sb.append("     ====> " + path.action).append( " coming from:");
                for(int v : path.vertices) {
                    sb.append(" ").append(graph.getName(v));
                }
                sb.append("\n");
            }
        }
        return sb.toString();
    }

    public void analyzeAnnotations() throws StandardException {
        for(int i = 0; i < annotations.length; ++i) {
            List<Path> paths = annotations[i].paths;
            if(paths.size() <= 1) {
                continue; // single delete path, no chance for conflicts.
            }
            while(!paths.isEmpty()) {
                Path needle = paths.remove(0);
                for(Path p : paths) {
                    if(p.action == needle.action) {
                        continue; // action is the same, this is probably fine (e.g. CASCADE from two paths.)
                    } else {
                        if(p.intersects(needle)) {
                            throw StandardException.newException(SQLState.LANG_DELETE_RULE_VIOLATION,
                                                           newConstraintName,
                                                           String.format("adding this foreign key leads to the conflicting delete actions on the table '%s' coming from this path '%s' and this path '%s'",
                                                                         graph.getName(i), p.toString(graph), needle.toString(graph)));
                        }
                    }
                }
            }
        }
    }
}
