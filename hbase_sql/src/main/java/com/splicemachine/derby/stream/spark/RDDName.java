package com.splicemachine.derby.stream.spark;

public enum RDDName {
    SINGLE_ROW_DATA_SET("Prepare Single Row Data Set"),
    EMPTY_DATA_SET("Prepare Data Set"),
    GET_VALUES("Read Values"),
    SUBTRACT_BY_KEY("Subtract Right From Left"),
    UNION("Perform Union");

    private final String stringValue;

    RDDName(String stringValue) {
        this.stringValue = stringValue;
    }

    public String displayName() {
        return stringValue;
    }

}
