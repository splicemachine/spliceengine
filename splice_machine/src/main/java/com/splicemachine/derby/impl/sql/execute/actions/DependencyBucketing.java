/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
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

package com.splicemachine.derby.impl.sql.execute.actions;
import splice.com.google.common.collect.*;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Helper class to sort objects to drop during drop schema cascade into buckets.
 * We do not want to drop objects one by one, as we would need to inform other region servers
 * every time we drop an object, so we want to drop them wave by wave. Each wave should contain
 * objects that do not depend on each other.
 * Objects that do not depend on anything can be part of the first wave.
 */
public class DependencyBucketing<T> extends DDLConstantOperation {
    private Multimap<T, T> dependencies = ArrayListMultimap.create();
    private Map<T, Integer> nodeLevels = new HashMap<>();
    private int maxLevel = 1;

    /**
     * Add object to the dependency graph. If the object is not yet in the graph, it should
     * be part of the first wave
     * @param node
     */
    void addSingleNode(T node) {
        nodeLevels.putIfAbsent(node, 0);
    }

    /**
     * Add parent and child to the dependency graph. IF neither are in the graph when this function
     * is called, they will respectively be part of the first and the second wave.
     * If parent was already part of a certain bucket, child must go in the following one, and propagate
     * that to its children.
     * @param parent
     * @param child
     */
    void addDependency(T parent, T child) {
        dependencies.put(parent, child);
        nodeLevels.putIfAbsent(parent, 0);
        nodeLevels.putIfAbsent(child, 1);
        propagateLevels(parent);
    }

    List<List<T>> getBuckets() {
        List<List<T>> buckets = new ArrayList<>(maxLevel + 1);
        for (int i = 0; i <= maxLevel; ++i) {
            buckets.add(new ArrayList<>());
        }
        for (Map.Entry<T, Integer> level: nodeLevels.entrySet()) {
            buckets.get(level.getValue()).add(level.getKey());
        }
        return buckets;
    }

    private void propagateLevels(T node) {
        for (T child : dependencies.get(node)) {
            if (nodeLevels.get(child) <= nodeLevels.get(node)) {
                nodeLevels.put(child, nodeLevels.get(node) + 1);
                maxLevel = Integer.max(nodeLevels.get(node) + 1, maxLevel);
                propagateLevels(child);
            }
        }
    }
}
