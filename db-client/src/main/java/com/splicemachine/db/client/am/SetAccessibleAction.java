/*
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.client.am;

// Java 2 PrivilegedExceptionAction encapsulation of the action to set accessible flag of an object

public class SetAccessibleAction implements java.security.PrivilegedExceptionAction {
    private boolean accessible_ = false;

    // provides information about, and dynamic access to, a single field of a class or an interface
    private java.lang.reflect.Field field_ = null;

    public SetAccessibleAction(java.lang.reflect.Field field, boolean accessible) {
        field_ = field;
        accessible_ = accessible;
    }

    public Object run() {
        field_.setAccessible(accessible_);
        return null;
    }

    public void setAccessible(boolean accessible) {
        accessible_ = accessible;
    }

    public void setField(java.lang.reflect.Field field) {
        field_ = field;
    }
}
