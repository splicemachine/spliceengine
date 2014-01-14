package com.splicemachine.test.utils;

import java.util.*;

/**
 * @author Jeff Cunningham
 *         Date: 9/16/13
 */
public class DependencyTree {

    private Map<String, DependencyNode> idView = new HashMap<String, DependencyNode>();

    public void addNode(DependencyNode node) {
        DependencyNode refNode = idView.get(node.id);
        if (refNode != null) {
            mergeNode(refNode, node);
        } else {
            idView.put(node.id, node);
        }
    }

    public List<String> resolveNodeNames(Collection<String> nodeIDs) {
        List<String> names = new ArrayList<String>();
        for (String nodeID : nodeIDs) {
            DependencyNode node = idView.get(nodeID);
            if (node != null) {
                names.add(node.name);
            }
        }
        return names;
    }

    public List<DependencyNode> getDependencyOrder() {
        List<DependencyNode> depOrder = new ArrayList<DependencyNode>(idView.size());
        Set<String> found = new HashSet<String>(idView.size());
        depthFirst(idView.keySet(), found, depOrder);
        return depOrder;
    }

    private void depthFirst(Set<String> parents, Set<String> found, List<DependencyNode> depOrder) {
        for (String parent : parents) {
            DependencyNode node = idView.get(parent);
            if (node != null) {
                depthFirst(node.depIDs, found, depOrder);
                if (! found.contains(node.id)) {
                    depOrder.add(node);
                    found.add(node.id);
                }
            }
        }
    }

    private void mergeNode(DependencyNode mergeNode, DependencyNode node) {
        mergeNode.depIDs.addAll(node.depIDs);
        mergeNode.parentIDs.addAll(node.parentIDs);
    }

    public static class DependencyNode {
        public final String id;
        public final String name;
        public final String type;
        public Set<String> depIDs = new HashSet<String>();
        public Set<String> parentIDs = new HashSet<String>();

        public DependencyNode(String name, String id, String type, String parentID, String dependentID) {
            if (name.contains(".")) {
                // must double quote table/view names with dots ('.') in them
                this.name = "\""+name+"\"";
            } else {
                this.name = name;
            }
            this.id = id;
            this.type = type;
            if (parentID != null && ! this.id.equals(parentID)) {
                parentIDs.add(parentID);
            }
            if (dependentID != null && ! this.id.equals(dependentID)) {
                depIDs.add(dependentID);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            DependencyNode node = (DependencyNode) o;

            if (id != null ? !id.equals(node.id) : node.id != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return id != null ? id.hashCode() : 0;
        }

        @Override
        public String toString() {
            return name + " " + type + " deps: "+depIDs+" parents: " + parentIDs;
        }
    }
}
