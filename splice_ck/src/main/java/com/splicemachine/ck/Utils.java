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

package com.splicemachine.ck;

import com.splicemachine.ck.command.common.CommonOptions;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.utils.marshall.dvd.*;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.utils.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableNotFoundException;

import java.io.UncheckedIOException;
import java.util.*;
import java.util.regex.Pattern;

import static com.splicemachine.ck.Constants.NULL;

public class Utils {

    public static Configuration constructConfig(String zkq, int port) {
        Configuration conf = HBaseConfiguration.create();
        conf.set(Constants.HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, zkq);
        conf.set(Constants.HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, Integer.toString(port));
        return conf;
    }

    public static class Tabular {

        public static enum SortHint {
            AsInteger,
            AsString
        }

        public static class Row implements Comparable<Row> {
            public List<String> cols;
            public SortHint sortHint;

            public Row(SortHint sortHint, String... cols) {
                assert cols.length > 0;
                this.sortHint = sortHint;
                this.cols = Arrays.asList(cols);
            }



            @Override
            public int compareTo(Row o) {
                if(o == null) {
                    return -1;
                }
                if(sortHint == SortHint.AsInteger) {
                    return Integer.compare(Integer.parseInt(cols.get(0)), Integer.parseInt(o.cols.get(0)));
                } else {
                    assert sortHint == SortHint.AsString;
                    return cols.get(0).compareTo(o.cols.get(0));
                }
            }

            @Override
            public boolean equals(Object obj) {
                if(!(obj instanceof Row)) {
                    return false;
                }
                return cols.equals(((Row)obj).cols) && sortHint ==((Row)obj).sortHint;
            }

            @Override
            public int hashCode() {
                return Objects.hash(cols, sortHint);
            }
        }

        public Set<Row> rows;
        public List<String> headers;
        SortHint sortHint;

        public Tabular(SortHint sortHint, String... headers) {
            assert headers.length > 0;
            this.sortHint = sortHint;
            this.headers = Arrays.asList(headers);
            rows = new TreeSet<>();
        }

        public void addRow(String... cols) {
            assert cols.length == headers.size();
            rows.add(new Row(sortHint, cols));
        }

        public List<String> getCol(int index) {
            assert index >= 0 && index < headers.size();
            List<String> result = new ArrayList<>(rows.size());
            for(final Row row : rows) {
                result.add(row.cols.get(index));
            }
            return result;
        }
    }

    public static String printTabularResults(Tabular tabular) {
        List<Integer> lengths = new ArrayList<>(tabular.headers.size());
        for(int i = 0; i < tabular.headers.size(); ++i) {
            lengths.add(tabular.headers.get(i).length());
        }
        for(Tabular.Row row : tabular.rows) {
            for(int i = 0; i < row.cols.size(); ++i) {
                if(lengths.get(i) < row.cols.get(i).length()) {
                    lengths.set(i, row.cols.get(i).length());
                }
            }
        }

        final int padding = 5;
        List<String> formats = new ArrayList<>(lengths.size());
        for(Integer length : lengths) {
            formats.add("%-" + (length + padding) + "s");
        }

        StringBuilder stringBuilder = new StringBuilder();
        for(int i = 0; i < tabular.headers.size(); ++i) {
            stringBuilder.append(Colored.boldWhite(String.format(formats.get(i), tabular.headers.get(i))));
        }
        stringBuilder.append("\n");
        int totalLength = lengths.stream().reduce(0, Integer::sum);
        totalLength += padding * lengths.size();
        String separate = new String(new char[totalLength]).replace("\0", "=");
        stringBuilder.append(Colored.boldWhite(separate)).append("\n");
        for(Tabular.Row row : tabular.rows) {
            for(int i = 0; i < row.cols.size(); ++i) {
                stringBuilder.append(String.format(formats.get(i), row.cols.get(i)));
            }
            stringBuilder.append("\n");
        }
        return stringBuilder.toString();
    }

    public static String toString(Pair<BitSet, ExecRow> p) throws StandardException {
        StringJoiner joiner = new StringJoiner(",");
        for(int i = 1; i <= p.getSecond().nColumns(); ++i) {
            if(p.getFirst().get(i - 1)) {
                joiner.add(p.getSecond().getColumn(i).toString());
            } else {
                joiner.add("[]");
            }
        }
        return joiner.toString();
    }

    public enum SQLType { BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, DOUBLE, REAL, NUMERIC,
        DECIMAL, CHAR, VARCHAR, REF, TIMESTAMP, DATE, TIME, LONG_VARCHAR, BLOB, CLOB, BIT,
        VARBIT, LONGVARBIT, XML, LIST }

    private static Map<String, SQLType> typeMap;
    static {
        typeMap = new HashMap<>();
        typeMap.put(TypeId.BOOLEAN_NAME, SQLType.BOOLEAN);
        typeMap.put(TypeId.TINYINT_NAME, SQLType.TINYINT);
        typeMap.put(TypeId.SMALLINT_NAME, SQLType.SMALLINT);
        typeMap.put(TypeId.INTEGER_NAME, SQLType.INT);
        typeMap.put(TypeId.LONGINT_NAME, SQLType.BIGINT);
        typeMap.put(TypeId.DECIMAL_NAME, SQLType.DECIMAL);
        typeMap.put(TypeId.NUMERIC_NAME, SQLType.NUMERIC);
        typeMap.put(TypeId.DOUBLE_NAME, SQLType.DOUBLE);
        typeMap.put(TypeId.REAL_NAME, SQLType.REAL);
        typeMap.put(TypeId.CHAR_NAME, SQLType.CHAR);
        typeMap.put(TypeId.VARCHAR_NAME, SQLType.VARCHAR);
        typeMap.put(TypeId.REF_NAME, SQLType.REF);
        typeMap.put(TypeId.LONGVARCHAR_NAME, SQLType.LONG_VARCHAR);
        typeMap.put(TypeId.BLOB_NAME, SQLType.BLOB);
        typeMap.put(TypeId.CLOB_NAME, SQLType.CLOB);
        typeMap.put(TypeId.DATE_NAME, SQLType.DATE);
        typeMap.put(TypeId.TIME_NAME, SQLType.TIME);
        typeMap.put(TypeId.TIMESTAMP_NAME, SQLType.TIMESTAMP);
        typeMap.put(TypeId.BIT_NAME, SQLType.BIT);
        typeMap.put(TypeId.VARBIT_NAME, SQLType.VARBIT);
        typeMap.put(TypeId.LONGVARBIT_NAME, SQLType.LONGVARBIT);
        typeMap.put(TypeId.XML_NAME, SQLType.XML);
        typeMap.put(TypeId.LIST_NAME, SQLType.LIST);
    }

    public static SQLType[] toSQLTypeArray(List<String> values) {
        SQLType[] result = new SQLType[values.size()];
        int cnt = 0;
        for(String value : values) {
            result[cnt++] = typeMap.get(value);
        }
        return result;
    }

    public static class Colored {

        private static final String ANSI_RESET = "\u001B[0m";
        private static final String ANSI_BLACK = "\u001B[30m";
        private static final String ANSI_RED = "\u001B[31m";
        private static final String ANSI_GREEN = "\u001B[32m";
        private static final String ANSI_YELLOW = "\u001B[33m";
        private static final String ANSI_BLUE = "\u001B[34m";
        private static final String ANSI_PURPLE = "\u001B[35m";
        private static final String ANSI_CYAN = "\u001B[36m";
        private static final String ANSI_WHITE = "\u001B[37m";
        private static final String ANSI_DARK_GRAY = "\u001B[30m";
        private static final String ANSI_WHITE_BOLD = "\033[1;37m";

        public static String green(String message) {
            return coloredInternal(ANSI_GREEN, message);
        }

        public static String red(String message) {
            return coloredInternal(ANSI_RED, message);
        }

        public static String blue(String message) {
            return coloredInternal(ANSI_BLUE, message);
        }

        public static String darkGray(String message) {
            return coloredInternal(ANSI_DARK_GRAY, message);
        }

        public static String yellow(String message) {
            return coloredInternal(ANSI_YELLOW, message);
        }

        public static String purple(String message) {
            return coloredInternal(ANSI_PURPLE, message);
        }

        public static String cyan(String message) {
            return coloredInternal(ANSI_CYAN, message);
        }

        public static String boldWhite(String message) {
            return coloredInternal(ANSI_WHITE_BOLD, message);
        }

        private static String coloredInternal(String color, String message) {
            if(CommonOptions.colors) {
                return color + message + ANSI_RESET;
            } else {
                return message;
            }
        }
    }

    public static String checkException(Exception e, String tableName) throws Exception {
        if (e instanceof TableNotFoundException || (e instanceof UncheckedIOException && e.getCause() instanceof TableNotFoundException)) {
            return Colored.red("table '" + tableName + "' not found.");
        } else {
            throw e;
        }
    }

    public static Pair<ExecRow, DescriptorSerializer[]> constructExecRowDescriptorSerializer(Utils.SQLType[] schema,
            int version, String[] values) throws StandardException {
        // map strings to format ids.
        SQLTimestamp.setSkipDBContext(true);
        SQLTime.setSkipDBContext(true);
        SQLDate.setSkipDBContext(true);
        assert values == null || values.length == schema.length;
        int[] storedFormatIds = new int[schema.length];
        DataValueDescriptor[] dataValueDescriptors = new DataValueDescriptor[schema.length];
        for (int i = 0; i < storedFormatIds.length; ++i) {
            switch (schema[i]) {
                case INT:
                    storedFormatIds[i] = StoredFormatIds.SQL_INTEGER_ID;
                    dataValueDescriptors[i] = new SQLInteger();
                    break;
                case BIGINT:
                    storedFormatIds[i] = StoredFormatIds.SQL_LONGINT_ID;
                    dataValueDescriptors[i] = new SQLLongint();
                    break;
                case DOUBLE:
                    storedFormatIds[i] = StoredFormatIds.SQL_DOUBLE_ID;
                    dataValueDescriptors[i] = new SQLDouble();
                    break;
                case CHAR:
                    storedFormatIds[i] = StoredFormatIds.SQL_CHAR_ID;
                    dataValueDescriptors[i] = new SQLChar();
                    break;
                case VARCHAR:
                    storedFormatIds[i] = StoredFormatIds.SQL_VARCHAR_ID;
                    dataValueDescriptors[i] = new SQLVarchar();
                    break;
                case TIME:
                    storedFormatIds[i] = StoredFormatIds.SQL_TIME_ID;
                    dataValueDescriptors[i] = new SQLTime();
                    break;
                case TIMESTAMP:
                    storedFormatIds[i] = StoredFormatIds.SQL_TIMESTAMP_ID;
                    dataValueDescriptors[i] = new SQLTimestamp();
                    break;
                case DATE:
                    storedFormatIds[i] = StoredFormatIds.SQL_DATE_ID;
                    dataValueDescriptors[i] = new SQLDate();
                    break;
                case DECIMAL:
                    storedFormatIds[i] = StoredFormatIds.SQL_DECIMAL_ID;
                    dataValueDescriptors[i] = new SQLDecimal();
                    break;
                default:
                    throw new RuntimeException("type not supported");
            }
            if(values != null) {
                dataValueDescriptors[i].setValue(values[i]);
            }
        }
        SerializerMap serializerMap = null;
        if (version == 1) {
            serializerMap = new V1SerializerMap(false);
        } else if (version == 2) {
            serializerMap = new V2SerializerMap(false);
        } else if (version == 3) {
            serializerMap = new V3SerializerMap(false);
        } else {
            serializerMap = new V4SerializerMap(false);
        }
        return new Pair<>(new ValueRow(dataValueDescriptors), serializerMap.getSerializers(storedFormatIds));
    }

    public static String[] splitUserDataInput(String value, String delimiter, String escape) {
        assert value != null;
        assert delimiter != null;
        assert escape != null;
        String regex = "(?<!" + Pattern.quote(escape) + ")" + Pattern.quote(delimiter);
        return value.split(regex);
    }

    public static byte[] createFakeSIAttribute(long txnId) {
        MultiFieldEncoder encoder=MultiFieldEncoder.create(9)
                .encodeNext(txnId)
                .encodeNext(txnId)
                .encodeNext(false)
                .encodeNext(Txn.IsolationLevel.SNAPSHOT_ISOLATION.encode())
                .encodeNext(true);
        encoder.encodeNext(-1).encodeNext(-1).encodeNext(-1);
        encoder.setRawBytes(Encoding.EMPTY_BYTE_ARRAY);
        return encoder.build();
    }

    public static void tell(String... what) {
        if(CommonOptions.verbose) {
            forceTell(what);
        }
    }

    public static void forceTell(String... what) {
        System.err.println(Colored.yellow(String.join(" ", what)));
        System.err.flush();
    }

    public static String checkNull(final String value) {
        return value == null ? NULL : value;
    }
}
