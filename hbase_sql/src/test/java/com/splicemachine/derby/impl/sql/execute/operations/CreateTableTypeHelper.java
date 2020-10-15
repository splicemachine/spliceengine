package com.splicemachine.derby.impl.sql.execute.operations;

import org.junit.Assert;

import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

/**
 * a helper class to define column types, and then create external tables and insert data into them
 * to make writing tests for all column types easier.
 */
public class CreateTableTypeHelper {
    /**
     *
     * @param types     an array of Types that should be used
     * @param ivalues   an array of values to use. 0 is NULL value, all other values will
     * generate corresponding entries that are somewhat associated with the integer val
     * e.g. mostly if the int values increase, the corresponding e.g. date value also increases.
     */
    public CreateTableTypeHelper(int[] types, int[] ivalues)
    {
        schema = Arrays.stream(types).mapToObj(this::getTypesName).collect(Collectors.joining(", "));
        suggestedTypes = Arrays.stream(types).mapToObj(this::getTypesNameInfered).collect(Collectors.joining(", "));

        IntFunction<String> iValueStringFunc = ivalue -> Arrays.stream(types)
                .mapToObj(type -> getTypeValue(type, ivalue))
                .collect(Collectors.joining(", "));

        values2 = Arrays.stream(ivalues).mapToObj(iValueStringFunc).sorted().collect(Collectors.toList());
        insertValues = Arrays.stream(ivalues).mapToObj(iValueStringFunc).map(s -> "(" + s + ")").collect(Collectors.joining(", "));
    }

    public String getInsertValues() {
        return insertValues;
    }



    /**
     * @return schema as to be used in `create external table <NAME> ( <SCHEMA> ) ...`
     */
    public String getSchema()
    {
        return schema;
    }

    /**
     *
     * @return schema that will be returned when suggesting a schema to the user.
     */
    public String getSuggestedTypes() {
        return suggestedTypes;
    }


    /**
     * @return types that are supported by Parquet
     */
    static public int[] getParquetTypes() {
        return new int[]{
                Types.VARCHAR, Types.CHAR,
                Types.DATE, Types.TIMESTAMP,
                // Types.TIME, // supported, but will be written as TIMESTAMP
                Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT,
                Types.DOUBLE, Types.REAL, Types.DECIMAL, Types.BOOLEAN
        };
    }

    /**
     * @return types that are supported by ORC
     */
    static public int[] getORCTypes() {
        return new int[]{
                Types.VARCHAR, Types.CHAR,
                Types.DATE, Types.TIMESTAMP,
                // Types.TIME, // supported, but will be written as TIMESTAMP
                Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT,
                Types.DOUBLE, Types.REAL, Types.DECIMAL, Types.BOOLEAN
        };
    }

    /**
     *
     * @return types that are supported by Avro
     */
    static public int[] getAvroTypes() {
        return new int[]{
                Types.VARCHAR, Types.CHAR,
                // Types.DATE, Types.TIME // not supported
                Types.TIMESTAMP,
                //Types.TINYINT, Types.SMALLINT, // not supported
                Types.INTEGER, Types.BIGINT,
                Types.DOUBLE, Types.REAL,
                // Types.DECIMAL, // not supported
                Types.BOOLEAN
        };
    }

    ///

    /**
     * @param fileFormat "PARQUET", "ORC" or "AVRO"
     * @return supported types by fileFormat PARQUET, ORC or AVRO
     */
    static public int[] getTypes(String fileFormat) {
        if(fileFormat.equalsIgnoreCase("PARQUET"))
            return getParquetTypes();
        else if(fileFormat.equalsIgnoreCase("ORC"))
            return getORCTypes();
        else if(fileFormat.equalsIgnoreCase("AVRO"))
            return getAvroTypes();
        throw new RuntimeException("unsupported fileformat " + fileFormat);
    }

    public static List<String> getListResult(ResultSet rs) throws SQLException {
        List<String> results = new ArrayList<>();
        int nCols = rs.getMetaData().getColumnCount();
        while (rs.next()) {
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i <= nCols; i++) {
                Object value = rs.getObject(i);
                if (value != null && value instanceof Clob) {
                    throw new RuntimeException("Clob not supported");
                } else {
                    if( i > 1 ) sb.append(", ");
                    if( value != null && value.toString() != null ) {
                        sb.append("'");
                        sb.append(value.toString());
                        sb.append("'");
                    }
                    else {
                        sb.append("NULL");
                    }
                }
            }
            results.add(sb.toString());
        }
        results.sort(String::compareTo);
        return results;
    }
    /**
     * compare that result in ResultSet rs is the same as from the generated insert values.
     * @param rs ResultSet from a select *.
     * @throws SQLException
     */
    public void checkResultSetSelectAll(ResultSet rs) throws SQLException {
        List<String> results = getListResult(rs);
        Assert.assertEquals( values2.size(), results.size() );
        for( int i = 0; i < results.size(); i++ ) {
            Assert.assertEquals( values2.get(i), results.get(i) );
        }
    }

    private String getTypesName(int type) {
        switch (type) {
            case Types.CHAR:
                return "COL_CHAR CHAR(10)";
            case Types.VARCHAR:
                return "COL_VARCHAR VARCHAR(10)";
            case Types.DATE:
                return "COL_DATE DATE";
            case Types.TIME:
                return "COL_TIME TIME";
            case Types.TIMESTAMP:
                return "COL_TIMESTAMP TIMESTAMP";
            case Types.TINYINT:
                return "COL_TINYINT TINYINT";
            case Types.SMALLINT:
                return "COL_SMALLINT SMALLINT";
            case Types.INTEGER:
                return "COL_INTEGER INT";
            case Types.BIGINT:
                return "COL_BIGINT BIGINT";
            //case Types.NUMERIC: Decimal and Numeric should be the same
            case Types.DECIMAL:
                return "COL_DECIMAL DECIMAL(7,2)";
            case Types.DOUBLE:
                return "COL_DOUBLE DOUBLE";
            case Types.REAL:
                return "COL_REAL REAL";
            case Types.BOOLEAN:
                return "COL_BOOLEAN BOOLEAN";
            default:
                throw new RuntimeException("unsupported type " + type);
        }
    }

    private String getTypesNameInfered(int type) {
        switch (type) {
            case Types.CHAR:
                return "COL_CHAR CHAR/VARCHAR(x)";
            case Types.VARCHAR:
                return "COL_VARCHAR CHAR/VARCHAR(x)";
            default:
                return getTypesName(type);
        }
    }
    private String getTime( int val ){
        if( val < 0 ) val = -val;
        int seconds = val;
        int minutes = seconds / 60;
        int hours = minutes / 60;
        seconds %= 60;
        minutes %= 60;
        hours %= 60;
        return String.format("%d:%02d:%02d", hours, minutes, seconds);
    }
    private String getDate( int val ){
        int days = val;
        int months = days / 28;
        int years = months / 12;
        days = days%28 + 1;
        months = months%12 + 1;
        years += 2000;
        return String.format("%d-%02d-%02d", years, months, days);
    }

    private String getTypeValue(int type, int val) {
        // DB-9829: BOOLEAN can't use NULL together with non-null
        // ERROR 42X61: Types 'CHAR' and 'BOOLEAN' are not UNION compatible.
        // Note this is not a problem with external tables
        if( type != Types.BOOLEAN
                && val == 0 ) { return "NULL"; }
        switch (type) {
            case Types.CHAR:
                return "'" + padSpaces("AAAA " + val, 10) + "'";
            case Types.VARCHAR:
                return "'AAAA " + val + "'";
            case Types.TIME:
                return "'" + getTime(val) + "'";
            case Types.DATE:
                return "'" + getDate(val) + "'";
            case Types.TIMESTAMP:
                return "'" + getDate(val) + " 09:45:01.123'";
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
                return "'" + val + "'";
            case Types.DOUBLE:
            case Types.REAL:
                return "'" + (val * 0.001 + 1.5) + "'";
            case Types.DECIMAL:
                return "'" + ((val % 10000) * 0.01 + 1.5) + "'";
            case Types.BOOLEAN:
                return val % 2 == 0 ? "'true'" : "'false'";
            default:
                throw new RuntimeException("unsupported type " + type);
        }
    }

    private String padSpaces(String s, int i) {
        StringBuilder sBuilder = new StringBuilder(s);
        while( sBuilder.length() < i ) sBuilder.append(" ");
        s = sBuilder.toString();
        return s;
    }

    DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ENGLISH);
    private final List<String> values2;
    private final String schema;
    private final String suggestedTypes;
    private final String insertValues;
}
