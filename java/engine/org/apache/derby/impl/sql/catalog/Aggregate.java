package org.apache.derby.impl.sql.catalog;

/**
 * Created with IntelliJ IDEA.
 * User: jyuan
 * Date: 12/16/13
 * Time: 12:52 PM
 * To change this template use File | Settings | File Templates.
 */
import org.apache.derby.catalog.AliasInfo;
import org.apache.derby.catalog.TypeDescriptor;
import org.apache.derby.catalog.UUID;
import org.apache.derby.catalog.types.AggregateAliasInfo;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.dictionary.AliasDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.store.access.TransactionController;

public class Aggregate {

    private String name;
    private TypeDescriptor inType;
    private TypeDescriptor returnType;
    private String javaClassName;

    public Aggregate() {}

    public Aggregate(String name, TypeDescriptor inType, TypeDescriptor returnType, String javaClassName) {
        this.name = name;
        this.inType = inType;
        this.returnType = returnType;
        this.javaClassName = javaClassName;
    }

    public void createSystemAggregate( DataDictionary dataDictionary, TransactionController tc)
    throws StandardException {
        char aliasType = AliasInfo.ALIAS_TYPE_AGGREGATE_AS_CHAR;
        char namespace = AliasInfo.ALIAS_NAME_SPACE_AGGREGATE_AS_CHAR;

        AggregateAliasInfo aliasInfo = new AggregateAliasInfo(inType, returnType);

        UUID aggregate_uuid = dataDictionary.getUUIDFactory().createUUID();

        UUID schemaId = dataDictionary.getSystemUtilSchemaDescriptor().getUUID();

        AliasDescriptor ads =
                new AliasDescriptor(
                        dataDictionary,
                        aggregate_uuid,
                        name,
                        schemaId,
                        javaClassName,
                        aliasType,
                        namespace,
                        false,
                        aliasInfo,
                        null);
        dataDictionary.addDescriptor(
                ads, null, DataDictionary.SYSALIASES_CATALOG_NUM, false, tc);
    }
}
