package com.splicemachine.db.impl.sql;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;

/**
 * Created by yxia on 8/20/19.
 */
public class SpliceTable extends AbstractQueryableTable implements TranslatableTable, ScannableTable {
    public final SpliceSchema spliceSchema;
    public final String schemaName;
    public final String tableName;
    public final Schema.TableType tableType;


    SpliceTable(SpliceSchema spliceSchema, String schemaName, String tableName, Schema.TableType tableType) {
        super(Object[].class);
        this.spliceSchema = spliceSchema;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.tableType = tableType;
    }

    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.FieldInfoBuilder b = typeFactory.builder();
        b.add("A1", typeFactory.createJavaType(Integer.class));
        b.add("B1", typeFactory.createJavaType(Integer.class));
        b.add("C1", typeFactory.createJavaType(Integer.class));

        return b.build();
    }

    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        return null;
    }

    public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
                                        SchemaPlus schema, String tableName) {
        return null;
    }

    public Enumerable<Object[]> scan(DataContext root) {
        return null;
    }
}
