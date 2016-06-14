package com.splicemachine.sql;

import java.math.BigDecimal;

/**
 * @author Scott Fines
 *         Date: 1/28/15
 */
public abstract class NumberColumn extends BaseColumn {
    private final boolean autoIncrement;
    private final boolean isCurrency;
    private final int precision;
    private final int scale;

    public NumberColumn(String className,
                        int precision,
                        int scale,
                        boolean searchable,
                        boolean autoIncrement,
                        boolean isCurrency,
                        ColumnNullability nullable,
                        String label,
                        String name,
                        String catalog,
                        String schema,
                        String table,
                        SQLType type,
                        boolean readOnly) {
        super(searchable, nullable, precision+1+scale==0?0:scale+1,
                label, name, catalog, schema, table, type, readOnly,className);
        this.autoIncrement = autoIncrement;
        this.isCurrency = isCurrency;
        this.precision = precision;
        this.scale = scale;
    }

    @Override public boolean isCurrency() { return isCurrency; }
    @Override public boolean isAutoIncrement() { return autoIncrement; }
    @Override public boolean isSigned() { return true; }
    @Override public int getScale() { return scale; }
    @Override public int getPrecision() { return precision; }
    @Override public boolean isCaseSensitive() { return false; }

    /*****************************************************************************************************************/
    /*factory methods*/
    public static ColumnMetaData tinyintColumn(String catalog,String schema,String table,String column,
                                               String colLabel,
                                               ColumnNullability nullability,
                                               boolean readOnly,
                                               boolean searchable,
                                               boolean autoIncrement,
                                               boolean isCurrency){
        return new ByteColumn(searchable,autoIncrement,isCurrency,
                nullability,colLabel,column,catalog,schema,table,readOnly);
    }

    public static ColumnMetaData smallintColumn(String catalog,String schema,String table,String column,
                                                String colLabel,
                                                ColumnNullability nullability,
                                                boolean readOnly,
                                                boolean searchable,
                                                boolean autoIncrement,
                                                boolean isCurrency){
        return new ShortColumn(searchable,autoIncrement,isCurrency,
                nullability,colLabel,column,catalog,schema,table,readOnly);
    }

    public static ColumnMetaData intColumn(String catalog,String schema,String table,String column,
                                           String colLabel,
                                           ColumnNullability nullability,
                                           boolean readOnly,
                                           boolean searchable,
                                           boolean autoIncrement,
                                           boolean isCurrency){
        return new IntColumn(searchable,autoIncrement,isCurrency,
                nullability,colLabel,column,catalog,schema,table,readOnly);
    }

    public static ColumnMetaData bigintColumn(String catalog,String schema,String table,String column,
                                              String colLabel,
                                              ColumnNullability nullability,
                                              boolean readOnly,
                                              boolean searchable,
                                              boolean autoIncrement,
                                              boolean isCurrency){
        return new LongColumn(searchable,autoIncrement,isCurrency,
                nullability,colLabel,column,catalog,schema,table,readOnly);
    }

    public static ColumnMetaData realColumn(String catalog,String schema,String table,String column,
                                              String colLabel,
                                              ColumnNullability nullability,
                                              boolean readOnly,
                                              boolean searchable,
                                              boolean autoIncrement,
                                              boolean isCurrency){
        return new RealColumn(searchable,autoIncrement,isCurrency,
                nullability,colLabel,column,catalog,schema,table,readOnly);
    }

    public static ColumnMetaData doubleColumn(String catalog,String schema,String table,String column,
                                            String colLabel,
                                            ColumnNullability nullability,
                                            boolean readOnly,
                                            boolean searchable,
                                            boolean autoIncrement,
                                            boolean isCurrency){
        return new DoubleColumn(searchable,autoIncrement,isCurrency,
                nullability,colLabel,column,catalog,schema,table,readOnly);
    }

    public static ColumnMetaData numericColumn(String catalog,String schema,String table,String column,
                                              String colLabel,
                                              int precision, int scale,
                                              ColumnNullability nullability,
                                              boolean readOnly,
                                              boolean searchable,
                                              boolean autoIncrement,
                                              boolean isCurrency){
        return new NumericColumn(precision,scale,searchable,autoIncrement,
                isCurrency,nullability,colLabel,column,catalog,schema,table,readOnly);
    }

    public static ColumnMetaData floatColumn(String catalog,String schema,String table,String column,
                                               String colLabel,
                                               int precision,
                                               ColumnNullability nullability,
                                               boolean readOnly,
                                               boolean searchable,
                                               boolean autoIncrement,
                                               boolean isCurrency){
        if(precision<23) return new RealColumn(searchable,autoIncrement,isCurrency,
                nullability,colLabel,column,catalog,schema,table,readOnly){
            @Override public SQLType getType() { return SQLType.FLOAT; }
        };
        else return new DoubleColumn(searchable, autoIncrement,isCurrency,
                nullability,colLabel,column,catalog,schema,table,readOnly){
            @Override public SQLType getType() { return SQLType.FLOAT; }
        };
    }

    /*****************************************************************************************************************/
    /*implementation classes*/
    private static class BitColumn extends NumberColumn{
        private static final String className = Byte.class.getName();
        private static final int precision = 1;

        public BitColumn(boolean searchable,
                          boolean autoIncrement,
                          boolean isCurrency,
                          ColumnNullability nullable,
                          String label,
                          String name,
                          String catalog,
                          String schema,
                          String table,
                          boolean readOnly) {
            super(className,
                    precision,
                    0,
                    searchable,
                    autoIncrement,
                    isCurrency,
                    nullable,
                    label,
                    name,
                    catalog,
                    schema,
                    table,
                    SQLType.BIT,
                    readOnly);
        }
    }

    private static class ByteColumn extends NumberColumn{
        private static final String className = Byte.class.getName();
        private static final int precision = Byte.toString(Byte.MAX_VALUE).length();

        public ByteColumn(boolean searchable,
                          boolean autoIncrement,
                          boolean isCurrency,
                          ColumnNullability nullable,
                          String label,
                          String name,
                          String catalog,
                          String schema,
                          String table,
                          boolean readOnly) {
            super(className,
                    precision,
                    0,
                    searchable,
                    autoIncrement,
                    isCurrency,
                    nullable,
                    label,
                    name,
                    catalog,
                    schema,
                    table,
                    SQLType.TINYINT,
                    readOnly);
        }
    }

    private static class ShortColumn extends NumberColumn{
        private static final String className = Short.class.getName();
        private static final int precision = Short.toString(Short.MAX_VALUE).length();

        public ShortColumn(boolean searchable,
                           boolean autoIncrement,
                           boolean isCurrency,
                           ColumnNullability nullable,
                           String label,
                           String name,
                           String catalog,
                           String schema,
                           String table,
                           boolean readOnly) {
            super(className,
                    precision,
                    0,
                    searchable,
                    autoIncrement,
                    isCurrency,
                    nullable,
                    label,
                    name,
                    catalog,
                    schema,
                    table,
                    SQLType.SMALLINT,
                    readOnly);
        }
    }

    private static class IntColumn extends NumberColumn{
        private static final String className = Integer.class.getName();
        private static final int precision = 10;

        public IntColumn(boolean searchable,
                         boolean autoIncrement,
                         boolean isCurrency,
                         ColumnNullability nullable,
                         String label,
                         String name,
                         String catalog,
                         String schema,
                         String table,
                         boolean readOnly) {
            super(className,
                    precision,
                    0,
                    searchable,
                    autoIncrement,
                    isCurrency,
                    nullable,
                    label,
                    name,
                    catalog,
                    schema,
                    table,
                    SQLType.INTEGER,
                    readOnly);
        }
    }

    private static class LongColumn extends NumberColumn {
        private static final String className = Long.class.getName();
        private static final int precision = Long.toString(Long.MAX_VALUE).length();

        public LongColumn(boolean searchable,
                          boolean autoIncrement,
                          boolean isCurrency,
                          ColumnNullability nullable,
                          String label,
                          String name,
                          String catalog,
                          String schema,
                          String table,
                          boolean readOnly) {
            super(className,
                    precision,
                    0,
                    searchable,
                    autoIncrement,
                    isCurrency,
                    nullable,
                    label,
                    name,
                    catalog,
                    schema,
                    table,
                    SQLType.BIGINT,
                    readOnly);
        }
    }

    private static class RealColumn extends NumberColumn {
        private static final String className = Float.class.getName();
        private static final int precision = Float.toString(Float.MAX_VALUE).length();
        private static final int scale = Float.toString(Float.MIN_NORMAL).length();

        public RealColumn(boolean searchable,
                          boolean autoIncrement,
                          boolean isCurrency,
                          ColumnNullability nullable,
                          String label,
                          String name,
                          String catalog,
                          String schema,
                          String table,
                          boolean readOnly) {
            super(className,
                    precision,
                    scale,
                    searchable,
                    autoIncrement,
                    isCurrency,
                    nullable,
                    label,
                    name,
                    catalog,
                    schema,
                    table,
                    SQLType.REAL,
                    readOnly);
        }
    }

    private static class DoubleColumn extends NumberColumn {
        private static final String className = Double.class.getName();
        private static final int precision = Double.toString(Double.MAX_VALUE).length();
        private static final int scale = Double.toString(Double.MIN_NORMAL).length();

        public DoubleColumn(boolean searchable,
                          boolean autoIncrement,
                          boolean isCurrency,
                          ColumnNullability nullable,
                          String label,
                          String name,
                          String catalog,
                          String schema,
                          String table,
                          boolean readOnly) {
            super(className,
                    precision,
                    scale,
                    searchable,
                    autoIncrement,
                    isCurrency,
                    nullable,
                    label,
                    name,
                    catalog,
                    schema,
                    table,
                    SQLType.DOUBLE,
                    readOnly);
        }
    }

    private static class NumericColumn extends NumberColumn{
        private static final String className = BigDecimal.class.getName();

        public NumericColumn( int precision, int scale,
                             boolean searchable,
                             boolean autoIncrement,
                             boolean isCurrency,
                             ColumnNullability nullable,
                             String label,
                             String name,
                             String catalog,
                             String schema,
                             String table,
                             boolean readOnly) {
            super(className,
                    precision,  scale,
                    searchable,
                    autoIncrement,
                    isCurrency,
                    nullable,
                    label,
                    name,
                    catalog,
                    schema,
                    table,
                    SQLType.NUMERIC,
                    readOnly);
        }
    }

    private static class DecimalColumn extends NumberColumn{
        private static final String className = BigDecimal.class.getName();

        public DecimalColumn( int precision, int scale,
                              boolean searchable,
                              boolean autoIncrement,
                              boolean isCurrency,
                              ColumnNullability nullable,
                              String label,
                              String name,
                              String catalog,
                              String schema,
                              String table,
                              boolean readOnly) {
            super(className,
                    precision,  scale,
                    searchable,
                    autoIncrement,
                    isCurrency,
                    nullable,
                    label,
                    name,
                    catalog,
                    schema,
                    table,
                    SQLType.DECIMAL,
                    readOnly);
        }
    }
}
