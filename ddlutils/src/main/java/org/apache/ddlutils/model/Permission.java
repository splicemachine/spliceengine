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
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

/**
 * A database permission defined by an Actor that has been granted authority to perform some Action on some Resource.
 */
public class Permission extends Grantable {

    private final String id;
    private final String resourceName;
    private final ResourceType resourceType;

    private Collection<Action> actions = new TreeSet<>(new Comparator<Action>() {
        @Override
        public int compare(Action a1, Action a2) {
            final Collator collator = Collator.getInstance();
            return collator.compare(a1.type, a2.type);
        }
    });

    public Permission(String id, String resourceName, ResourceType resourceType) {
        this.id = id;
        this.resourceName = resourceName;
        this.resourceType = resourceType;
    }

    public String getId() {
        return id;
    }

    public ResourceType getResourceType() {
        return resourceType;
    }

    public String getResourceName() {
        return resourceName;
    }

    public Collection<Action> getActions() {
        return actions;
    }

    public void addAction(String action) {
        actions.add(Action.of(action));
    }

    public void addAction(Action action) {
        actions.add(action);
    }

    public boolean hasAllPrivileges() {
        return actions.containsAll(Action.allActions());
    }

    public boolean hasPrivilege(Action action) {
        return actions.contains(action);
    }

    /**
     * A database Resource on which can be granted an Action.
     */
    public enum ResourceType {
        TABLE,
        FUNCTION,
        PROCEDURE;
    }

    /**
     * An Action that can be performed on a Resource.
     */
    public enum Action {
        SELECTPRIV("SELECT"),
        DELETEPRIV("DELETE"),
        INSERTPRIV("INSERT"),
        UPDATEPRIV("UPDATE"),
        REFERENCESPRIV("REFERENCES"),
        TRIGGERPRIV("TRIGGER");

        private String type;

        Action(String type) {
            this.type = type;
        }

        public String type() {
            return type;
        }

        private final static Map<String, Action> map = new HashMap<>(Action.values().length, 1.0f);
        static {
            for (Action t : Action.values()) {
                map.put(t.type, t);
            }
        }

        public static Action of(String name) {
            Action result = map.get(name);
            if (result == null) {
                throw new IllegalArgumentException("No Action Exists for '"+name+"'");
            }
            return result;
        }

        public static Collection<Action> allActions() {
            return map.values();
        }
    }
}
