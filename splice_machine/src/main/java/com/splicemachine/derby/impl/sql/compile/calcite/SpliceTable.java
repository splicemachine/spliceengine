package com.splicemachine.derby.impl.sql.compile.calcite;

import com.splicemachine.db.catalog.TypeDescriptor;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.ColumnDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ColumnDescriptorList;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.derby.impl.sql.compile.calcite.reloperators.SpliceRelNode;
import com.splicemachine.derby.impl.sql.compile.calcite.reloperators.SpliceTableScan;
import com.splicemachine.db.impl.sql.compile.FromBaseTable;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;

/**
 * Created by yxia on 8/20/19.
 */
public class SpliceTable extends AbstractQueryableTable implements TranslatableTable, ScannableTable {
    public final SpliceSchema spliceSchema;
    public final String schemaName;
    public final String tableName;
    public final Schema.TableType tableType;
    private LanguageConnectionContext lcc;
    private TableDescriptor tableDescriptor;
    private int tableNumber;
    private FromBaseTable fromBaseTable;


    SpliceTable(SpliceSchema spliceSchema,
                String schemaName,
                String tableName,
                Schema.TableType tableType,
                TableDescriptor td,
                LanguageConnectionContext lcc) {
        super(Object[].class);
        this.spliceSchema = spliceSchema;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.tableType = tableType;
        this.tableDescriptor = td;
        this.lcc = lcc;
    }

    // use logic similar to that in JdbcTable to implement getRowType
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        final RelDataTypeFactory protoTypeFactory =
                new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RelDataTypeFactory.Builder fieldInfo = protoTypeFactory.builder();

        ColumnDescriptorList cdl=tableDescriptor.getColumnDescriptorList();
        ColumnDescriptor colDesc;
        for(int index=0;index<cdl.size();index++) {
            colDesc = cdl.elementAt(index);
            String columnName = colDesc.getColumnName();
            TypeDescriptor typeDescriptor = colDesc.getType().getCatalogType();
            int jdbcTypeId = typeDescriptor.getJDBCTypeId();
            int precision = typeDescriptor.getPrecision();
            int scale = typeDescriptor.getScale();
           // String typeString = typeDescriptor.getSQLstring();
            RelDataType relDataType = sqlType(typeFactory, jdbcTypeId, precision, scale);
            boolean nullable = typeDescriptor.isNullable();
            fieldInfo.add(columnName, relDataType).nullable(nullable);
        }

        RelProtoDataType protoRowType = RelDataTypeImpl.proto(fieldInfo.build());
        return protoRowType.apply(typeFactory);
    }

    private RelDataType sqlType(RelDataTypeFactory typeFactory, int dataType,
                                int precision, int scale) {
        // Fall back to ANY if type is unknown
        final SqlTypeName sqlTypeName =
                Util.first(SqlTypeName.getNameForJdbcType(dataType), SqlTypeName.ANY);
        if (precision >= 0
                && scale >= 0
                && sqlTypeName.allowsPrecScale(true, true)) {
            return typeFactory.createSqlType(sqlTypeName, precision, scale);
        } else if (precision >= 0 && sqlTypeName.allowsPrecNoScale()) {
            return typeFactory.createSqlType(sqlTypeName, precision);
        } else {
            assert sqlTypeName.allowsNoPrecNoScale();
            return typeFactory.createSqlType(sqlTypeName);
        }
    }

    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        final RelOptCluster cluster = context.getCluster();
        final TableScan scan = new SpliceTableScan(cluster, cluster.traitSetOf(SpliceRelNode.CONVENTION), relOptTable);
        return scan;
    }

    public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
                                        SchemaPlus schema, String tableName) {
        return null;
    }

    public Enumerable<Object[]> scan(DataContext root) {
        return null;
    }

    public void setTableNumber(int tableNumber) {
        this.tableNumber = tableNumber;
    }

    public int getTableNumber() {
        return tableNumber;
    }

    public void setFromBaseTableNode(FromBaseTable fromBaseTable) {
        this.fromBaseTable = fromBaseTable;
    }

    public FromBaseTable getFromBaseTableNode() {
        return fromBaseTable;
    }
}
