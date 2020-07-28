package com.splicemachine.ck;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.TypeId;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableNotFoundException;

import java.io.UncheckedIOException;
import java.util.*;

import static com.splicemachine.db.iapi.types.TypeId.BOOLEAN_NAME;

public class Utils {

    public static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    public static final String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";

    public static Configuration constructConfig(String zkq, int port) {
        Configuration conf = HBaseConfiguration.create();
        conf.set(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, zkq);
        conf.set(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, Integer.toString(port));
        return conf;
    }

    public static class Tabular {
        public static class Row implements Comparable<Row> {
            public List<String> cols;

            public Row(String... cols) {
                assert cols.length > 0;
                this.cols = Arrays.asList(cols);
            }

            @Override
            public int compareTo(Row o) {
                return cols.get(0).compareTo(o.cols.get(0));
            }
        }

        public Set<Row> rows;
        public List<String> headers;

        public Tabular(String... headers) {
            assert headers.length > 0;
            this.headers = Arrays.asList(headers);
            rows = new TreeSet<>();
        }

        public void addRow(String... cols) {
            assert cols.length == headers.size();
            rows.add(new Row(cols));
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

        List<String> formats = new ArrayList<>(lengths.size());
        for(Integer length : lengths) {
            formats.add("%-" + (length + 5) + "s");
        }

        StringBuilder stringBuilder = new StringBuilder();
        for(int i = 0; i < tabular.headers.size(); ++i) {
            stringBuilder.append(Colored.boldWhite(String.format(formats.get(i), tabular.headers.get(i))));
        }
        stringBuilder.append("\n");
        int totalLength = lengths.stream().reduce(0, Integer::sum);
        totalLength += 5 * lengths.size();
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

    public static String toString(ExecRow er) throws StandardException {
        StringJoiner joiner = new StringJoiner(",");
        for(int i = 1; i <= er.nColumns(); ++i) {
            joiner.add(er.getColumn(i).toString());
        }
        return joiner.toString();
    }

    public enum SQLType { BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, DOUBLE, REAL, NUMERIC,
        DECIMAL, CHAR, VARCHAR, REF, TIMESTAMP, DATE, TIME, LONG_VARCHAR, BLOB, CLOB, BIT,
        VARBIT, LONGVARBIT, XML, LIST, UNKNOWN }

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

        public static final String ANSI_RESET = "\u001B[0m";
        public static final String ANSI_BLACK = "\u001B[30m";
        public static final String ANSI_RED = "\u001B[31m";
        public static final String ANSI_GREEN = "\u001B[32m";
        public static final String ANSI_YELLOW = "\u001B[33m";
        public static final String ANSI_BLUE = "\u001B[34m";
        public static final String ANSI_PURPLE = "\u001B[35m";
        public static final String ANSI_CYAN = "\u001B[36m";
        public static final String ANSI_WHITE = "\u001B[37m";
        public static final String ANSI_DARK_GRAY = "\u001B[30m";
        public static final String ANSI_WHITE_BOLD = "\033[1;37m";

        public static String green(String message) {
            return ANSI_GREEN + message + ANSI_RESET;
        }

        public static String red(String message) {
            return ANSI_RED + message + ANSI_RESET;
        }

        public static String blue(String message) {
            return ANSI_BLUE + message + ANSI_RESET;
        }

        public static String darkGray(String message) {
            return ANSI_DARK_GRAY + message + ANSI_RESET;
        }

        public static String yellow(String message) {
            return ANSI_YELLOW  + message + ANSI_RESET;
        }

        public static String purple(String message) {
            return ANSI_PURPLE + message + ANSI_RESET;
        }

        public static String cyan(String message) {
            return ANSI_CYAN + message + ANSI_RESET;
        }

        public static String boldWhite(String message) {
            return ANSI_WHITE_BOLD + message + ANSI_RESET;
        }
    }

    public static String checkException(Exception e, String tableName) throws Exception {
        if (e instanceof TableNotFoundException || (e instanceof UncheckedIOException && e.getCause() instanceof TableNotFoundException)) {
            return Colored.red("table '" + tableName + "' not found.");
        } else {
            throw e;
        }
    }
}
