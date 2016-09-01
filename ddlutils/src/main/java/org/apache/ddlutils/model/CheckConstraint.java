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

/**
 * Represents a database check constraint. A check constraint can be on a table or a column.
 */

public class CheckConstraint {
    private final String constraintName;
    private final String checkDefinition;

    public CheckConstraint(String constraintName, String checkDefinition) {
        if (checkDefinition == null || checkDefinition.isEmpty()) {
            throw new IllegalArgumentException("Check constraint definition must be given.");
        }
        this.constraintName = constraintName;
        this.checkDefinition = checkDefinition;
    }

    public String getCheckDefinition() {
        return checkDefinition;
    }

    public String getConstraintName() {
        return constraintName;
    }

    @Override
    public String toString() {
        return (constraintName != null && ! constraintName.isEmpty() ? constraintName : "")+" CHECK "+checkDefinition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CheckConstraint that = (CheckConstraint) o;

        return constraintName != null ? constraintName.equals(that.constraintName) : that.constraintName == null &&
            checkDefinition.equals(that.checkDefinition);

    }

    @Override
    public int hashCode() {
        int result = constraintName != null ? constraintName.hashCode() : 0;
        result = 31 * result + checkDefinition.hashCode();
        return result;
    }
}
