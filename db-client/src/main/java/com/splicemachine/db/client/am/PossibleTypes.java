/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.client.am;

/**
 * @author Scott Fines
 *         Date: 9/21/16
 */

import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.db.shared.common.sanity.SanityManager;

import java.sql.SQLException;
import java.util.Arrays;

/**
 * PossibleTypes is information which is set of types.
 * A given type is evaluated as *possible* at checkType method if same type was found in the set.
 */
public enum PossibleTypes{

    /**
     * This is possibleTypes of variable which can be set by set method for generic scalar.
     */
    POSSIBLE_TYPES_IN_SET_GENERIC_SCALAR(new int[] {
                    java.sql.Types.BIGINT,
                    java.sql.Types.LONGVARCHAR ,
                    java.sql.Types.CHAR,
                    java.sql.Types.DECIMAL,
                    java.sql.Types.INTEGER,
                    java.sql.Types.SMALLINT,
                    java.sql.Types.REAL,
                    java.sql.Types.DOUBLE,
                    java.sql.Types.VARCHAR,
                    java.sql.Types.BOOLEAN } ),

    /**
     * This is possibleTypes of variable which can be set by setDate method.
     */
    POSSIBLE_TYPES_IN_SET_DATE( new int[] {
                    java.sql.Types.LONGVARCHAR,
                    java.sql.Types.CHAR,
                    java.sql.Types.VARCHAR,
                    java.sql.Types.DATE,
                    java.sql.Types.TIMESTAMP }),

    /**
     * This is possibleTypes of variable which can be set by setTime method.
     */
    POSSIBLE_TYPES_IN_SET_TIME(new int[] {
                    java.sql.Types.LONGVARCHAR,
                    java.sql.Types.CHAR,
                    java.sql.Types.VARCHAR,
                    java.sql.Types.TIME } ),

    /**
     * This is possibleTypes of variable which can be set by setTimestamp method.
     */
    POSSIBLE_TYPES_IN_SET_TIMESTAMP(new int[] {
                    java.sql.Types.LONGVARCHAR,
                    java.sql.Types.CHAR,
                    java.sql.Types.VARCHAR,
                    java.sql.Types.DATE,
                    java.sql.Types.TIME,
                    java.sql.Types.TIMESTAMP } ),

    /**
     * This is possibleTypes of variable which can be set by setString method.
     */
    POSSIBLE_TYPES_IN_SET_STRING(new int[] {
                    java.sql.Types.BIGINT,
                    java.sql.Types.LONGVARCHAR,
                    java.sql.Types.CHAR,
                    java.sql.Types.DECIMAL,
                    java.sql.Types.INTEGER,
                    java.sql.Types.SMALLINT,
                    java.sql.Types.REAL,
                    java.sql.Types.DOUBLE,
                    java.sql.Types.VARCHAR,
                    java.sql.Types.BOOLEAN,
                    java.sql.Types.DATE,
                    java.sql.Types.TIME,
                    java.sql.Types.TIMESTAMP,
                    java.sql.Types.CLOB } ),

    /**
     * This is possibleTypes of variable which can be set by setBytes method.
     */
    POSSIBLE_TYPES_IN_SET_BYTES( new int[] {
                    java.sql.Types.LONGVARBINARY,
                    java.sql.Types.VARBINARY,
                    java.sql.Types.BINARY,
                    java.sql.Types.LONGVARCHAR,
                    java.sql.Types.CHAR,
                    java.sql.Types.VARCHAR,
                    java.sql.Types.BLOB } ),

    /**
     * This is possibleTypes of variable which can be set by setBinaryStream method.
     */
    POSSIBLE_TYPES_IN_SET_BINARYSTREAM( new int[] {
                    java.sql.Types.LONGVARBINARY,
                    java.sql.Types.VARBINARY,
                    java.sql.Types.BINARY,
                    java.sql.Types.BLOB } ),

    /**
     * This is possibleTypes of variable which can be set by setAsciiStream method.
     */
    POSSIBLE_TYPES_IN_SET_ASCIISTREAM( new int[]{
                    java.sql.Types.LONGVARCHAR,
                    java.sql.Types.CHAR,
                    java.sql.Types.VARCHAR,
                    java.sql.Types.CLOB } ),

    /**
     * This is possibleTypes of variable which can be set by setCharacterStream method.
     */
    POSSIBLE_TYPES_IN_SET_CHARACTERSTREAM( new int[] {
                    java.sql.Types.LONGVARCHAR,
                    java.sql.Types.CHAR,
                    java.sql.Types.VARCHAR,
                    java.sql.Types.CLOB } ),

    /**
     * This is possibleTypes of variable which can be set by setBlob method.
     */
    POSSIBLE_TYPES_IN_SET_BLOB( new int[] {
                    java.sql.Types.BLOB } ),

    /**
     * This is possibleTypes of variable which can be set by setClob method.
     */
    POSSIBLE_TYPES_IN_SET_CLOB( new int[] {
                    java.sql.Types.CLOB } ),

    /**
     * This is possibleTypes of null value which can be assigned to generic scalar typed variable.
     */
    POSSIBLE_TYPES_FOR_GENERIC_SCALAR_NULL( new int[] {
                    java.sql.Types.BIT,
                    java.sql.Types.TINYINT,
                    java.sql.Types.BIGINT,
                    java.sql.Types.LONGVARCHAR,
                    java.sql.Types.CHAR,
                    java.sql.Types.NUMERIC,
                    java.sql.Types.DECIMAL,
                    java.sql.Types.INTEGER,
                    java.sql.Types.SMALLINT,
                    java.sql.Types.FLOAT,
                    java.sql.Types.REAL,
                    java.sql.Types.DOUBLE,
                    java.sql.Types.VARCHAR } ),

    /**
     * This is possibleTypes of null value which can be assigned to generic character typed variable.
     */
    POSSIBLE_TYPES_FOR_GENERIC_CHARACTERS_NULL( new int[] {
                    java.sql.Types.BIT,
                    java.sql.Types.TINYINT,
                    java.sql.Types.BIGINT,
                    java.sql.Types.LONGVARCHAR,
                    java.sql.Types.CHAR,
                    java.sql.Types.NUMERIC,
                    java.sql.Types.DECIMAL,
                    java.sql.Types.INTEGER,
                    java.sql.Types.SMALLINT,
                    java.sql.Types.FLOAT,
                    java.sql.Types.REAL,
                    java.sql.Types.DOUBLE,
                    java.sql.Types.VARCHAR,
                    java.sql.Types.DATE,
                    java.sql.Types.TIME,
                    java.sql.Types.TIMESTAMP } ),

    /**
     * This is possibleTypes of null value which can be assigned to VARBINARY typed variable.
     */
    POSSIBLE_TYPES_FOR_VARBINARY_NULL ( new int[] {
                    java.sql.Types.VARBINARY,
                    java.sql.Types.BINARY,
                    java.sql.Types.LONGVARBINARY } ),

    /**
     * This is possibleTypes of null value which can be assigned to BINARY typed variable.
     */
    POSSIBLE_TYPES_FOR_BINARY_NULL ( new int[] {
                    java.sql.Types.VARBINARY,
                    java.sql.Types.BINARY,
                    java.sql.Types.LONGVARBINARY } ),

    /**
     * This is possibleTypes of null value which can be assigned to LONGVARBINARY typed variable.
     */
    POSSIBLE_TYPES_FOR_LONGVARBINARY_NULL ( new int[] {
                    java.sql.Types.VARBINARY,
                    java.sql.Types.BINARY,
                    java.sql.Types.LONGVARBINARY } ),

    /**
     * This is possibleTypes of null value which can be assigned to DATE typed variable.
     */
    POSSIBLE_TYPES_FOR_DATE_NULL ( new int[] {
                    java.sql.Types.LONGVARCHAR,
                    java.sql.Types.CHAR,
                    java.sql.Types.VARCHAR,
                    java.sql.Types.DATE,
                    java.sql.Types.TIMESTAMP } ),

    /**
     * This is possibleTypes of null value which can be assigned to TIME typed variable.
     */
    POSSIBLE_TYPES_FOR_TIME_NULL ( new int[] {
                    java.sql.Types.LONGVARCHAR,
                    java.sql.Types.CHAR,
                    java.sql.Types.VARCHAR,
                    java.sql.Types.TIME,
                    java.sql.Types.TIMESTAMP } ),

    /**
     * This is possibleTypes of null value which can be assigned to TIMESTAMP typed variable.
     */
    POSSIBLE_TYPES_FOR_TIMESTAMP_NULL ( new int[] {
                    java.sql.Types.LONGVARCHAR,
                    java.sql.Types.CHAR,
                    java.sql.Types.VARCHAR,
                    java.sql.Types.DATE,
                    java.sql.Types.TIMESTAMP } ),

    /**
     * This is possibleTypes of null value which can be assigned to CLOB typed variable.
     */
    POSSIBLE_TYPES_FOR_CLOB_NULL ( new int[] {
                    java.sql.Types.LONGVARCHAR,
                    java.sql.Types.CHAR,
                    java.sql.Types.VARCHAR,
                    java.sql.Types.CLOB } ),

    /**
     * This is possibleTypes of null value which can be assigned to BLOB typed variable.
     */
    POSSIBLE_TYPES_FOR_BLOB_NULL ( new int[] { java.sql.Types.BLOB } ),

    /**
     * This is possibleTypes of null value which can be assigned to other typed variable.
     */
    DEFAULT_POSSIBLE_TYPES_FOR_NULL ( new int[] {
                    java.sql.Types.BIT,
                    java.sql.Types.TINYINT,
                    java.sql.Types.BIGINT,
                    java.sql.Types.LONGVARBINARY,
                    java.sql.Types.VARBINARY,
                    java.sql.Types.BINARY,
                    java.sql.Types.LONGVARCHAR,
                    java.sql.Types.NULL,
                    java.sql.Types.CHAR,
                    java.sql.Types.NUMERIC,
                    java.sql.Types.DECIMAL,
                    java.sql.Types.INTEGER,
                    java.sql.Types.SMALLINT,
                    java.sql.Types.FLOAT,
                    java.sql.Types.REAL,
                    java.sql.Types.DOUBLE,
                    java.sql.Types.VARCHAR,
                    java.sql.Types.BOOLEAN,
                    java.sql.Types.DATALINK,
                    java.sql.Types.DATE,
                    java.sql.Types.TIME,
                    java.sql.Types.TIMESTAMP,
                    java.sql.Types.OTHER,
                    java.sql.Types.JAVA_OBJECT,
                    java.sql.Types.DISTINCT,
                    java.sql.Types.STRUCT,
                    java.sql.Types.ARRAY,
                    java.sql.Types.BLOB,
                    java.sql.Types.CLOB,
                    java.sql.Types.REF } );

    final private int[] possibleTypes;

    PossibleTypes(int[] types){
        possibleTypes = types;
        Arrays.sort(possibleTypes);
    }

    /**
     * This method return true if the type is possible.
     */
    boolean checkType(int type){
        if(SanityManager.DEBUG){
            for(int i = 0; i < possibleTypes.length - 1; i ++){
                SanityManager.ASSERT(possibleTypes[i] < possibleTypes[i + 1]);
            }
        }

        return Arrays.binarySearch( possibleTypes, type ) >= 0;
    }

    public void performTypeCheck(int paramType,int valueType) throws SQLException{
        if(!checkType(paramType)){
            throwtypeMismatch(valueType,paramType);
        }
    }
    public static void throwtypeMismatch(int valType, int paramType) throws SQLException{
        throw new SqlException( null,
                new ClientMessageId(SQLState.LANG_DATA_TYPE_GET_MISMATCH),
                new Object[]{
                        Types.getTypeString(valType),
                        Types.getTypeString(paramType)
                },
                (Throwable) null).getSQLException();
    }

    public static void throw22005Exception( LogWriter logWriter, int valType, int paramType) throws SqlException{
        throw new SqlException( logWriter,
                new ClientMessageId(SQLState.LANG_DATA_TYPE_GET_MISMATCH),
                new Object[]{
                        Types.getTypeString(valType),
                        Types.getTypeString(paramType)
                },
                (Throwable) null);
    }


    /**
     * This method return possibleTypes of null value in variable typed as typeOfVariable.
     */
    public static PossibleTypes getPossibleTypesForNull(int typeOfVariable){
        switch(typeOfVariable){
            case java.sql.Types.SMALLINT: return POSSIBLE_TYPES_FOR_GENERIC_SCALAR_NULL;
            case java.sql.Types.INTEGER: return POSSIBLE_TYPES_FOR_GENERIC_SCALAR_NULL;
            case java.sql.Types.BIGINT: return POSSIBLE_TYPES_FOR_GENERIC_SCALAR_NULL;
            case java.sql.Types.REAL: return POSSIBLE_TYPES_FOR_GENERIC_SCALAR_NULL;
            case java.sql.Types.FLOAT: return POSSIBLE_TYPES_FOR_GENERIC_SCALAR_NULL;
            case java.sql.Types.DOUBLE: return POSSIBLE_TYPES_FOR_GENERIC_SCALAR_NULL;
            case java.sql.Types.DECIMAL: return POSSIBLE_TYPES_FOR_GENERIC_SCALAR_NULL;
            case java.sql.Types.CHAR: return POSSIBLE_TYPES_FOR_GENERIC_CHARACTERS_NULL;
            case java.sql.Types.VARCHAR: return POSSIBLE_TYPES_FOR_GENERIC_CHARACTERS_NULL;
            case java.sql.Types.LONGVARCHAR: return POSSIBLE_TYPES_FOR_GENERIC_CHARACTERS_NULL;
            case java.sql.Types.VARBINARY: return POSSIBLE_TYPES_FOR_VARBINARY_NULL;
            case java.sql.Types.BINARY: return POSSIBLE_TYPES_FOR_BINARY_NULL;
            case java.sql.Types.LONGVARBINARY: return POSSIBLE_TYPES_FOR_LONGVARBINARY_NULL;
            case java.sql.Types.DATE: return POSSIBLE_TYPES_FOR_DATE_NULL;
            case java.sql.Types.TIME: return POSSIBLE_TYPES_FOR_TIME_NULL;
            case java.sql.Types.TIMESTAMP: return POSSIBLE_TYPES_FOR_TIMESTAMP_NULL;
            case java.sql.Types.CLOB: return POSSIBLE_TYPES_FOR_CLOB_NULL;
            case java.sql.Types.BLOB: return POSSIBLE_TYPES_FOR_BLOB_NULL;
            default:
                return DEFAULT_POSSIBLE_TYPES_FOR_NULL;
        }
    }

}

