package com.splicemachine.db.impl.sql.catalog;

import com.splicemachine.db.catalog.AliasInfo;
import com.splicemachine.db.catalog.TypeDescriptor;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.catalog.types.AggregateAliasInfo;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.AliasDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.store.access.TransactionController;

/**
 * Represents a system aggregate function in the data dictionary.
 * 
 * @author Jun Yuan
 */
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

    public void createSystemAggregate(DataDictionary dataDictionary, TransactionController tc)
		throws StandardException {
    	// By default, puts aggregate function into SYSCS_UTIL schema
    	// unless you invoke the method that takes schema UUID argument.
        UUID schemaId = dataDictionary.getSystemUtilSchemaDescriptor().getUUID();
        createSystemAggregate(dataDictionary, tc, schemaId);
    }

    public void createSystemAggregate(DataDictionary dataDictionary, TransactionController tc, UUID schemaId)
		throws StandardException {

        char aliasType = AliasInfo.ALIAS_TYPE_AGGREGATE_AS_CHAR;
        char namespace = AliasInfo.ALIAS_NAME_SPACE_AGGREGATE_AS_CHAR;

        AggregateAliasInfo aliasInfo = new AggregateAliasInfo(inType, returnType);

        UUID aggregate_uuid = dataDictionary.getUUIDFactory().createUUID();

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
