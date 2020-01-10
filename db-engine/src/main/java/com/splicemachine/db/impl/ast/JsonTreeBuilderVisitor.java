/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
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
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.ast;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.impl.sql.compile.QueryTreeNode;

import java.util.*;

/**
 * Visits the entire tree and, using node references passed in constructs a tree structure suitable use with vis.js
 * (http://visjs.org/).  After visiting the tree call toJson() and use the resulting JSON to visualize the graph.
 */
public class JsonTreeBuilderVisitor implements Visitor {

    private GraphContainer container = new GraphContainer();
    private int id;

    @Override
    public Visitable visit(Visitable currentVisitable, QueryTreeNode parent) throws StandardException {

        /* add the current currentVisitable to the tree if we don't have it already */
        Node currentNode = findByReference((QueryTreeNode) currentVisitable);
        if (currentNode == null) {
            currentNode = new Node(id++, (QueryTreeNode) currentVisitable);
            container.getNodes().add(currentNode);
        }

        /* If this node has a parent then add an edge */
        if (parent != null) {
            Node parentNode = findByReference(parent);
            if (parentNode == null) {
                throw new IllegalStateException("could not find parent for currentVisitable = " + currentVisitable);
            }
            container.getEdges().add(new Edge("", currentNode.getId(), parentNode.getId()));
        }

        return currentVisitable;
    }

    @Override
    public boolean visitChildrenFirst(Visitable node) {
        return false;
    }

    @Override
    public boolean stopTraversal() {
        return false;
    }

    @Override
    public boolean skipChildren(Visitable node) throws StandardException {
        return false;
    }

    public String toJson() {
        Gson gson = new GsonBuilder().create();
        return gson.toJson(container);
    }

    private Node findByReference(QueryTreeNode queryTreeNode) {
        for (Node node : container.getNodes()) {
            if (node.getQueryTreeNode() == queryTreeNode) {
                return node;
            }
        }
        return null;
    }

    // -----------------------------------------------------------------------------------------------------------------
    //
    // classes for json serialization - the properties of these private classes match the expected input format
    // of visjs. See http://visjs.org/
    //
    // -----------------------------------------------------------------------------------------------------------------

    private static class Edge {

        private int from;
        private int to;
        private String label;
        private String arrows = "to";

        public Edge(String label, int from, int to) {
            this.from = from;
            this.label = label;
            this.to = to;
        }

        public int getFrom() {
            return from;
        }

        public void setFrom(int from) {
            this.from = from;
        }

        public String getLabel() {
            return label;
        }

        public void setLabel(String label) {
            this.label = label;
        }

        public int getTo() {
            return to;
        }

        public void setTo(int to) {
            this.to = to;
        }

        public String getArrows() {
            return arrows;
        }

        public void setArrows(String arrows) {
            this.arrows = arrows;
        }

        private List<?> getIdentityFields() {
            return Arrays.asList(getFrom(), getTo(), getLabel(), getArrows());
        }

        @Override
        public boolean equals(Object other) {
            return (other == this) || (other instanceof Edge) && ((Edge) other).getIdentityFields().equals(this.getIdentityFields());
        }

        @Override
        public int hashCode() {
            return getIdentityFields().hashCode();
        }
    }

    private static class Node {

        private transient QueryTreeNode queryTreeNode;
        private int id;
        private String label;
        private String group;
        private String title;

        public Node(int id, QueryTreeNode queryTreeNode) {
            this.id = id;
            this.queryTreeNode = queryTreeNode;
            this.label = queryTreeNode.getClass().getSimpleName();
            this.group = this.label.toLowerCase();
            this.title = queryTreeNode.toHTMLString();
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getLabel() {
            return label;
        }

        public void setLabel(String label) {
            this.label = label;
        }

        public String getGroup() {
            return group;
        }

        public void setGroup(String group) {
            this.group = group;
        }

        public QueryTreeNode getQueryTreeNode() {
            return queryTreeNode;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }
    }

    private static class GraphContainer {

        private Set<Edge> edges = new HashSet<>();
        private List<Node> nodes = new ArrayList<>();

        public Set<Edge> getEdges() {
            return edges;
        }

        public List<Node> getNodes() {
            return nodes;
        }
    }


}
