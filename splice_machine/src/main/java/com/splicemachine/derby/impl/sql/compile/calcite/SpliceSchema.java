package com.splicemachine.derby.impl.sql.compile.calcite;

import com.google.common.collect.ImmutableList;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.materialize.Lattice;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.*;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

/**
 * Created by yxia on 8/20/19.
 */
public class SpliceSchema implements Schema {

    private LanguageConnectionContext lcc;
    private String schemaName;
    private boolean isRoot;
    private SchemaDescriptor schemaDescriptor;

    public SpliceSchema(LanguageConnectionContext lcc, String schemaName, SchemaDescriptor sd, boolean isRoot) {
        this.lcc = lcc;
        this.schemaName = schemaName;
        this.isRoot = isRoot;
        this.schemaDescriptor = sd;
    }

    public static SpliceSchema create(String name, SchemaDescriptor sd, LanguageConnectionContext lcc) {
        return new SpliceSchema(lcc, name, sd, false);

    }

    public Table getTable(String tableName) {
        try {
            if (schemaDescriptor == null)
                schemaDescriptor = lcc.getDataDictionary().getSchemaDescriptor(schemaName, lcc.getTransactionCompile(), true);
            TableDescriptor td=lcc.getDataDictionary().getTableDescriptor(tableName, schemaDescriptor, lcc.getTransactionCompile());
            SpliceTable table = null;
            if (td != null) {
                table = new SpliceTable(this, schemaName, td.getName(),  TableType.TABLE, td, lcc);
            }
            return table;
        } catch (StandardException e) {
            throw new RuntimeException(SQLState.LANG_TABLE_NOT_FOUND,e);
        }
    }

    public Set<String> getTableNames() {
        return Collections.emptySet();
    }

    public RelProtoDataType getType(String name) {
        return null;
    }

    public Set<String> getTypeNames() {
        return null;
    }

    public Collection<Function> getFunctions(String name) {
        return null;
    }

    public Set<String> getFunctionNames() {
        return null;
    }

    public Schema getSubSchema(String schemaName) {
        // splice does not support nested schemas
        if (isRoot) {
            try {
                SchemaDescriptor sd = lcc.getDataDictionary().getSchemaDescriptor(schemaName, lcc.getTransactionCompile(), true);
                SpliceSchema subSchema = null;
                if (sd != null) {
                    subSchema = create(sd.getSchemaName(), sd, lcc);
                }
                return subSchema;
            } catch (StandardException e) {
                throw new RuntimeException(SQLState.LANG_SCHEMA_DOES_NOT_EXIST, e);
            }
        }

        return null;
    }

    public Set<String> getSubSchemaNames() {
        return Collections.emptySet();
    }

    public Expression getExpression(SchemaPlus parentSchema, String name) {
        return null;
    }

    public boolean isMutable() {
        return false;
    }

    public Schema snapshot(SchemaVersion version) {
        return null;
    }
    public Schema add(String name, Schema schema){
        return null;
    }

    public void add(String name, Table table){

    }

    public void add(String name, RelProtoDataType type) {

    }

    public void add(String name, Function function) {

    }

    /** Adds a lattice to this schema. */
    public void add(String name, Lattice lattice) {

    }


    /** Returns an underlying object. */
    public <T> T unwrap(Class<T> clazz) {
        return null;
    }

    public void setPath(ImmutableList<ImmutableList<String>> path) {

    }

    public String getName() {
        return schemaName;
    }

}
