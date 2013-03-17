package com.splicemachine.perf.runner;

import java.sql.PreparedStatement;
import java.sql.Types;

/**
 * @author Scott Fines
 *         Created on: 3/15/13
 */
public enum ColumnType {
    INTEGER("int",Types.INTEGER),
    VARCHAR("varchar",Types.VARCHAR){
        @Override public boolean requiresWidth() { return true; }
    };

    private final int jdbcTypeCode;
    private final String typeName;

    private ColumnType(String typeName,int jdbcTypeCode) {
        this.jdbcTypeCode = jdbcTypeCode;
        this.typeName = typeName;
    }

    public static ColumnType parse(int jdbcTypeCode){
        for(ColumnType type:values()){
            if(type.jdbcTypeCode==jdbcTypeCode) return type;
        }
        throw new AssertionError("Unable to parse jdbc type code "+ jdbcTypeCode+ " into a column type");
    }

    public static ColumnType parse(String typeName){
        for(ColumnType type:values()){
            if(type.typeName.equalsIgnoreCase(typeName))return type;
        }

        throw new AssertionError("Unable to parse column typeName "+ typeName);
    }

    public String getTypeName() {
        return typeName;
    }

    public boolean requiresWidth() {
        return false;
    }

}
