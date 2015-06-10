package com.splicemachine.test_dao;

import com.google.common.base.Predicate;

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
