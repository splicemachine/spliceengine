package com.splicemachine.db.impl.sql;

import com.google.common.collect.ImmutableList;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.materialize.Lattice;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.*;

import java.util.Collection;
import java.util.Set;

/**
 * Created by yxia on 8/20/19.
 */
public class SpliceSchema implements SchemaPlus {

    private LanguageConnectionContext lcc;
    private String schemaName;

    public SpliceSchema(LanguageConnectionContext lcc, String schemaName) {
        this.lcc = lcc;
        this.schemaName = schemaName;
    }

    public static SpliceSchema create(String name, LanguageConnectionContext lcc) {
        return new SpliceSchema(lcc, name);

    }

    public Table getTable(String tableName) {
        try {
            SchemaDescriptor sd = lcc.getDataDictionary().getSchemaDescriptor(schemaName, lcc.getTransactionCompile(), true);
            TableDescriptor td=lcc.getDataDictionary().getTableDescriptor(tableName,sd, lcc.getTransactionCompile());
            SpliceTable table = null;
            if (td != null) {
                table = new SpliceTable(this, schemaName, td.getName(),  TableType.TABLE);
            }
            return table;
        } catch (StandardException e) {

        }

        return null;
    }

    public Set<String> getTableNames() {
        return null;
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

    public SchemaPlus getSubSchema(String name) {
        return null;
    }

    public Set<String> getSubSchemaNames() {
        return null;
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

    public SchemaPlus getParentSchema() {
        return null;
    }

    public SchemaPlus add(String name, Schema schema){
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

    public void setCacheEnabled(boolean cache) {

    }

    public boolean isCacheEnabled() {
        return false;
    }

    public String getName() {
        return schemaName;
    }
}
