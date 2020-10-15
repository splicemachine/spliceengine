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

package com.splicemachine.test_dao;

import splice.com.google.common.base.Predicate;

public class Constraint {

    private String constraintId;
    private String tableId;
    private String constraintName;
    private String type;
    private String schemaId;
    private String state;
    private int referenceCount;

    public String getConstraintId() {
        return constraintId;
    }

    public void setConstraintId(String constraintId) {
        this.constraintId = constraintId;
    }

    public String getConstraintName() {
        return constraintName;
    }

    public void setConstraintName(String constraintName) {
        this.constraintName = constraintName;
    }

    public int getReferenceCount() {
        return referenceCount;
    }

    public void setReferenceCount(int referenceCount) {
        this.referenceCount = referenceCount;
    }

    public String getSchemaId() {
        return schemaId;
    }

    public void setSchemaId(String schemaId) {
        this.schemaId = schemaId;
    }

    public String getTableId() {
        return tableId;
    }

    public void setTableId(String tableId) {
        this.tableId = tableId;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "Constraint{" +
                "constraintId='" + constraintId + '\'' +
                ", tableId='" + tableId + '\'' +
                ", constraintName='" + constraintName + '\'' +
                ", type='" + type + '\'' +
                ", schemaId='" + schemaId + '\'' +
                ", state='" + state + '\'' +
                ", referenceCount=" + referenceCount +
                '}';
    }

    public static Predicate<Constraint> constraintTypePredicate(final String type) {
        return new Predicate<Constraint>() {
            @Override
            public boolean apply(Constraint constraint) {
                return type.equalsIgnoreCase(constraint.getType());
            }
        };
    }
}
