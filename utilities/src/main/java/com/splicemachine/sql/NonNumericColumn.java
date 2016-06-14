package com.splicemachine.sql;

/**
 * @author Scott Fines
 *         Date: 1/28/15
 */
public class NonNumericColumn extends BaseColumn{
    private final int maxLength;
    private final boolean caseSensitive;

    protected NonNumericColumn(int maxLength,
                               boolean caseSensitive,
                               boolean searchable,
                               ColumnNullability nullable,
                               String label,
                               String name,
                               String catalog,
                               String schema,
                               String table,
                               SQLType type,
                               boolean readOnly,
                               String colClassName) {
        super(searchable, nullable, maxLength, label, name, catalog, schema, table, type, readOnly, colClassName);
        this.maxLength = maxLength;
        this.caseSensitive = caseSensitive;
    }

    @Override public boolean isAutoIncrement() { return false; }
    @Override public int getScale() { return 0; }
    @Override public boolean isSigned() { return false; }
    @Override public boolean isCurrency() { return false; }
    @Override public boolean isCaseSensitive() { return caseSensitive; }
    @Override public int getPrecision() { return maxLength; }

    private static final String STRING_CLASS_NAME = String.class.getName();
    private static final String BYTES_CLASS_NAME = byte[].class.getName();

    public static ColumnMetaData charColumn(int maxLength,
                                            boolean caseSensitive,
                                            boolean searchable,
                                            ColumnNullability nullability,
                                            String catalog,
                                            String schema,
                                            String table,
                                            String column,
                                            String colLabel,
                                            boolean readOnly){
        return new NonNumericColumn(maxLength,caseSensitive,
                searchable,nullability,
                colLabel,column,catalog,schema,table,
                SQLType.CHAR,readOnly,STRING_CLASS_NAME);
    }

    public static ColumnMetaData varcharColumn(int maxLength,
                                            boolean caseSensitive,
                                            boolean searchable,
                                            ColumnNullability nullability,
                                            String catalog,
                                            String schema,
                                            String table,
                                            String column,
                                            String colLabel,
                                            boolean readOnly){
        return new NonNumericColumn(maxLength,caseSensitive,
                searchable,nullability,
                colLabel,column,catalog,schema,table,
                SQLType.VARCHAR,readOnly,STRING_CLASS_NAME);
    }

    public static ColumnMetaData longvarcharColumn(int maxLength,
                                               boolean caseSensitive,
                                               boolean searchable,
                                               ColumnNullability nullability,
                                               String catalog,
                                               String schema,
                                               String table,
                                               String column,
                                               String colLabel,
                                               boolean readOnly){
        return new NonNumericColumn(maxLength,caseSensitive,
                searchable,nullability,
                colLabel,column,catalog,schema,table,
                SQLType.LONGVARCHAR,readOnly,STRING_CLASS_NAME);
    }

    public static ColumnMetaData ncharColumn(int maxLength,
                                            boolean caseSensitive,
                                            boolean searchable,
                                            ColumnNullability nullability,
                                            String catalog,
                                            String schema,
                                            String table,
                                            String column,
                                            String colLabel,
                                            boolean readOnly){
        return new NonNumericColumn(maxLength,caseSensitive,
                searchable,nullability,
                colLabel,column,catalog,schema,table,
                SQLType.NCHAR,readOnly,STRING_CLASS_NAME);
    }

    public static ColumnMetaData nvarcharColumn(int maxLength,
                                               boolean caseSensitive,
                                               boolean searchable,
                                               ColumnNullability nullability,
                                               String catalog,
                                               String schema,
                                               String table,
                                               String column,
                                               String colLabel,
                                               boolean readOnly){
        return new NonNumericColumn(maxLength,caseSensitive,
                searchable,nullability,
                colLabel,column,catalog,schema,table,
                SQLType.NVARCHAR,readOnly,STRING_CLASS_NAME);
    }

    public static ColumnMetaData longnvarcharColumn(int maxLength,
                                                   boolean caseSensitive,
                                                   boolean searchable,
                                                   ColumnNullability nullability,
                                                   String catalog,
                                                   String schema,
                                                   String table,
                                                   String column,
                                                   String colLabel,
                                                   boolean readOnly){
        return new NonNumericColumn(maxLength,caseSensitive,
                searchable,nullability,
                colLabel,column,catalog,schema,table,
                SQLType.LONGNVARCHAR,readOnly,STRING_CLASS_NAME);
    }


    public static ColumnMetaData booleanColumn( boolean searchable,
                                                    ColumnNullability nullability,
                                                    String catalog,
                                                    String schema,
                                                    String table,
                                                    String column,
                                                    String colLabel,
                                                    boolean readOnly){
        return new NonNumericColumn(4,false,
                searchable,nullability,
                colLabel,column,catalog,schema,table,
                SQLType.BOOLEAN,readOnly,STRING_CLASS_NAME);
    }

    public static ColumnMetaData dateColumn( boolean searchable,
                                                ColumnNullability nullability,
                                                String catalog,
                                                String schema,
                                                String table,
                                                String column,
                                                String colLabel,
                                                boolean readOnly){
        return new NonNumericColumn(10,false,
                searchable,nullability,
                colLabel,column,catalog,schema,table,
                SQLType.DATE,readOnly,STRING_CLASS_NAME);
    }

    public static ColumnMetaData timeColumn( boolean searchable,
                                             ColumnNullability nullability,
                                             String catalog,
                                             String schema,
                                             String table,
                                             String column,
                                             String colLabel,
                                             boolean readOnly){
        return new NonNumericColumn(12,false,
                searchable,nullability,
                colLabel,column,catalog,schema,table,
                SQLType.TIME,readOnly,STRING_CLASS_NAME);
    }

    public static ColumnMetaData timestampColumn( boolean searchable,
                                             ColumnNullability nullability,
                                             String catalog,
                                             String schema,
                                             String table,
                                             String column,
                                             String colLabel,
                                             boolean readOnly){
        return new NonNumericColumn(24,false,
                searchable,nullability,
                colLabel,column,catalog,schema,table,
                SQLType.TIMESTAMP,readOnly,STRING_CLASS_NAME);
    }


    public static ColumnMetaData binaryColumn(int length, boolean searchable,
                                                  ColumnNullability nullability,
                                                  String catalog,
                                                  String schema,
                                                  String table,
                                                  String column,
                                                  String colLabel,
                                                  boolean readOnly){
        return new NonNumericColumn(length,false,
                searchable,nullability,
                colLabel,column,catalog,schema,table,
                SQLType.BINARY,readOnly,BYTES_CLASS_NAME);
    }

    public static ColumnMetaData varbinaryColumn(int maxLength, boolean searchable,
                                              ColumnNullability nullability,
                                              String catalog,
                                              String schema,
                                              String table,
                                              String column,
                                              String colLabel,
                                              boolean readOnly){
        return new NonNumericColumn(maxLength,false,
                searchable,nullability,
                colLabel,column,catalog,schema,table,
                SQLType.VARBINARY,readOnly,BYTES_CLASS_NAME);
    }

    public static ColumnMetaData longvarbinaryColumn(int maxLength, boolean searchable,
                                                 ColumnNullability nullability,
                                                 String catalog,
                                                 String schema,
                                                 String table,
                                                 String column,
                                                 String colLabel,
                                                 boolean readOnly){
        return new NonNumericColumn(maxLength,false,
                searchable,nullability,
                colLabel,column,catalog,schema,table,
                SQLType.LONGVARBINARY,readOnly,BYTES_CLASS_NAME);
    }

    public static ColumnMetaData nullColumn(boolean searchable,
                                                     ColumnNullability nullability,
                                                     String catalog,
                                                     String schema,
                                                     String table,
                                                     String column,
                                                     String colLabel,
                                                     boolean readOnly){
        return new NonNumericColumn(4,false,
                searchable,nullability,
                colLabel,column,catalog,schema,table,
                SQLType.NULL,readOnly,STRING_CLASS_NAME);
    }

    public static ColumnMetaData otherColumn(Class<?> clazz,int maxLength, boolean searchable,
                                                     ColumnNullability nullability,
                                                     String catalog,
                                                     String schema,
                                                     String table,
                                                     String column,
                                                     String colLabel,
                                                     boolean readOnly){
        return new NonNumericColumn(maxLength,false,
                searchable,nullability,
                colLabel,column,catalog,schema,table,
                SQLType.JAVA_OBJECT,readOnly,clazz.getName());
    }

}
