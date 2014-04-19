package org.apache.derby.impl.sql.catalog;

import org.apache.derby.catalog.AliasInfo;
import org.apache.derby.catalog.TypeDescriptor;
import org.apache.derby.catalog.UUID;
import org.apache.derby.catalog.types.RoutineAliasInfo;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.Limits;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.sql.dictionary.AliasDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.DataTypeDescriptor;

import java.sql.Types;
import java.util.*;

/**
 * @author Scott Fines
 *         Created on: 2/22/13
 */
public class DefaultSystemProcedureGenerator implements SystemProcedureGenerator,ModuleControl {
    private static final String SYSTEM_PROCEDURES = "org.apache.derby.catalog.SystemProcedures";
    private static final String LOB_STORED_PROCEDURE = "org.apache.derby.impl.jdbc.LOBStoredProcedure";

    private static final DataTypeDescriptor TYPE_SYSTEM_IDENTIFIER =
            DataTypeDescriptor.getBuiltInDataTypeDescriptor(
                    Types.VARCHAR, false, 128);

    private static final TypeDescriptor CATALOG_TYPE_SYSTEM_IDENTIFIER =
            TYPE_SYSTEM_IDENTIFIER.getCatalogType();

    private DataDictionary dictionary;

    public DefaultSystemProcedureGenerator(DataDictionary dictionary) {
        this.dictionary = dictionary;
    }

    public void boot(boolean create, Properties properties) throws StandardException{
        dictionary = (DataDictionary) Monitor.findSystemModule(DataDictionary.MODULE);
    }

    public void stop() {
        //no-op
    }


    protected static final int SYSIBM_PROCEDURES = 1;
    protected static final int SYSCS_PROCEDURES = 2;
    protected static final int SQLJ_PROCEDURES = 3;

//    @Override
    public final void createProcedures(TransactionController tc, HashSet newlyCreatedRoutines) throws StandardException {
        //get system procedures
        Map/*<UUID,Procedure>*/ procedureMap = getProcedures(dictionary,tc);
        Iterator uuidIterator = procedureMap.keySet().iterator();
        while(uuidIterator.hasNext()){
            UUID uuid = (UUID)uuidIterator.next();
            Iterator/*<Procedure>*/ procedures = ((List) procedureMap.get(uuid)).iterator();
            while(procedures.hasNext()){
                Object n = procedures.next();
                //free null check plus cast protection
                if(n instanceof Procedure){
                    Procedure procedure = (Procedure)n;
                    newlyCreatedRoutines.add(procedure.createSystemProcedure(uuid,dictionary,tc));
                }
            }
        }
    }

    /**
	 * Create or update a system stored procedure.  If the system stored procedure alreadys exists in the data dictionary,
	 * the stored procedure will be dropped and then created again.
	 * 
	 * @param schemaName           the schema where the procedure does and/or will reside
	 * @param procName             the procedure to create or update
	 * @param tc                   the xact
	 * @param newlyCreatedRoutines set of newly created procedures
	 * @throws StandardException
	 */
    public final void createOrUpdateProcedure(
    		String schemaName,
    		String procName,
    		TransactionController tc,
    		HashSet newlyCreatedRoutines) throws StandardException {

    	if (schemaName == null || procName == null) {
    		throw StandardException.newException(SQLState.LANG_OBJECT_NOT_FOUND_DURING_EXECUTION, "PROCEDURE", (schemaName + "." + procName));
    	}

    	// Delete the procedure from SYSALIASES if it already exists.
    	SchemaDescriptor sd = dictionary.getSchemaDescriptor(schemaName, tc, true);  // Throws an exception if the schema does not exist.
    	UUID schemaId = sd.getUUID();
    	AliasDescriptor ad = dictionary.getAliasDescriptor(schemaId.toString(), procName, AliasInfo.ALIAS_NAME_SPACE_PROCEDURE_AS_CHAR);
    	if (ad != null) {  // Drop the procedure if it already exists.
//    		System.out.println(String.format("Dropping already existing procedure: %s.%s", sd.getSchemaName(), procName));
    		dictionary.dropAliasDescriptor(ad, tc);
    	}

    	// Find the definition of the procedure and create it.
    	Procedure procedure = findProcedure(schemaId, procName, tc);
    	if (procedure == null) {
    		throw StandardException.newException(SQLState.LANG_OBJECT_NOT_FOUND_DURING_EXECUTION, "PROCEDURE", (schemaName + "." + procName));
    	} else {
//    		System.out.println(String.format("Creating procedure: %s.%s", sd.getSchemaName(), procName));
    		newlyCreatedRoutines.add(procedure.createSystemProcedure(schemaId, dictionary, tc));
    	}
    }

    /**
	 * Create or update all system stored procedures.  If the system stored procedure already exists in the data dictionary,
	 * the stored procedure will be dropped and then created again.
	 * 
	 * @param schemaName           the schema where the procedures do and/or will reside
	 * @param tc                   the xact
	 * @param newlyCreatedRoutines set of newly created procedures
	 * @throws StandardException
	 */
    public final void createOrUpdateAllProcedures(
    		String schemaName,
    		TransactionController tc,
    		HashSet newlyCreatedRoutines) throws StandardException {

    	if (schemaName == null) {
    		throw StandardException.newException(SQLState.LANG_OBJECT_NOT_FOUND_DURING_EXECUTION, "SCHEMA", (schemaName));
    	}

    	List procedureList = findProceduresForSchema(schemaName, tc);
    	Iterator/*<Procedure>*/ procedures = procedureList.iterator();
    	while (procedures.hasNext()) {
    		Object n = procedures.next();
    		if (n instanceof Procedure) {
    			Procedure procedure = (Procedure)n;
    			createOrUpdateProcedure(schemaName, procedure.getName(), tc, newlyCreatedRoutines);
    		}
    	}
    }

    /**
     * Find a list of system procedures for the specified schema that has been defined in this class.
     *
     * @param schemaName  name of the schema
     * @param tc          the transaction
     * @return            the system stored procedure if found, otherwise, null is returned
     * @throws StandardException
     */
    protected List findProceduresForSchema(String schemaName, TransactionController tc) throws StandardException {

    	if (schemaName == null || tc == null) {
    		return null;
    	}

    	SchemaDescriptor sd = dictionary.getSchemaDescriptor(schemaName, tc, true);  // Throws an exception if the schema does not exist.
    	UUID schemaId = sd.getUUID();
    	Map/*<UUID, List<Procedure>>*/ procedureMap = getProcedures(dictionary, tc);
    	if (procedureMap == null) {
    		return null;
    	}
    	return ((List) procedureMap.get(schemaId));
    }

    /**
     * Find a system procedure that has been defined in this class.
     *
     * @param schemaId  ID of the schema
     * @param procName  name of the system stored procedure
     * @param tc        the transaction
     * @return  the system stored procedure if found, otherwise, null is returned
     * @throws StandardException
     */
    protected Procedure findProcedure(UUID schemaId, String procName, TransactionController tc) throws StandardException {

    	if (schemaId == null || procName == null || tc == null) {
    		return null;
    	}
    	Map/*<UUID, List<Procedure>>*/ procedureMap = getProcedures(dictionary, tc);
    	if (procedureMap == null) {
    		return null;
    	}
    	List procedureList = (List) procedureMap.get(schemaId);
    	if (procedureList == null) {
    		return null;
    	}

    	Iterator/*<Procedure>*/ procedures = procedureList.iterator();
    	while (procedures.hasNext()) {
    		Object n = procedures.next();
    		if (n instanceof Procedure) {
    			Procedure procedure = (Procedure)n;
    			if (procName.equalsIgnoreCase(procedure.getName())) {
    				return procedure;
    			}
    		}
    	}
    	return null;
    }

    protected Map/*<UUID,List<Procedure>>*/ getProcedures(DataDictionary dictionary,TransactionController tc) throws StandardException {
        Map/*<UUID,Procedure>*/ procedures = new HashMap/*<UUID,Procedure>*/();

        //add SYSIBM
        UUID sysIbmUUID = dictionary.getSysIBMSchemaDescriptor().getUUID();
        procedures.put(sysIbmUUID,sysIbmProcedures);

        //add SQLJ
        UUID sqlJUUID = dictionary.getSchemaDescriptor(
                SchemaDescriptor.STD_SQLJ_SCHEMA_NAME,tc,true).getUUID();
        procedures.put(sqlJUUID,sqlJProcedures);

        //ADD SYSCS
        UUID sysUUID = dictionary.getSystemUtilSchemaDescriptor().getUUID();
        procedures.put(sysUUID,sysCsProcedures);

//        System.out.println(String.format("getProcedures: %s SYSIBM procs, %s SQLJ procs, %s SYSCS procs", sysIbmProcedures.size(), sqlJProcedures.size(), sysCsProcedures.size()));

        //TODO -sf- add 10_1-10_9 procedures
        return procedures;
    }

    private static final List/*<Procedure>*/ sysCsProcedures = new ArrayList/*<Procedure>*/(Arrays.asList(new Procedure[]{
            Procedure.newBuilder().name("SYSCS_SET_DATABASE_PROPERTY").numOutputParams(0).numResultSets(0)
                    .sqlControl(RoutineAliasInfo.MODIFIES_SQL_DATA).returnType(null).isDeterministic(false)
                    .ownerClass(SYSTEM_PROCEDURES)
                    .catalog("KEY")
                    .varchar("VALUE", Limits.DB2_VARCHAR_MAXWIDTH).build()
            ,
            Procedure.newBuilder().name("SYSCS_COMPRESS_TABLE").numOutputParams(0).numResultSets(0)
                    .sqlControl(RoutineAliasInfo.MODIFIES_SQL_DATA).returnType(null).isDeterministic(false)
                    .ownerClass(SYSTEM_PROCEDURES)
                    .catalog("SCHEMANAME")
                    .catalog("TABLENAME")
                    .smallint("SEQUENTIAL").build()
            ,
            Procedure.newBuilder().name("SYSCS_CHECKPOINT_DATABASE").numOutputParams(0).numResultSets(0)
                    .sqlControl(RoutineAliasInfo.CONTAINS_SQL).returnType(null).isDeterministic(false)
                    .ownerClass(SYSTEM_PROCEDURES).build()
            ,
            Procedure.newBuilder().name("SYSCS_FREEZE_DATABASE").numOutputParams(0).numResultSets(0)
                    .sqlControl(RoutineAliasInfo.CONTAINS_SQL).returnType(null).isDeterministic(false)
                    .ownerClass(SYSTEM_PROCEDURES).build()
            ,
            Procedure.newBuilder().name("SYSCS_UNFREEZE_DATABASE").numOutputParams(0).numResultSets(0)
                    .sqlControl(RoutineAliasInfo.CONTAINS_SQL).returnType(null).isDeterministic(false)
                    .ownerClass(SYSTEM_PROCEDURES).build()
            ,
            Procedure.newBuilder().name("SYSCS_BACKUP_DATABASE").numOutputParams(0).numResultSets(0)
                    .sqlControl(RoutineAliasInfo.MODIFIES_SQL_DATA).returnType(null).isDeterministic(false)
                    .ownerClass(SYSTEM_PROCEDURES)
                    .varchar("BACKUPDIR", Limits.DB2_VARCHAR_MAXWIDTH).build()
            ,
            Procedure.newBuilder().name("SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE")
                    .numOutputParams(0).numResultSets(0)
                    .sqlControl(RoutineAliasInfo.MODIFIES_SQL_DATA).returnType(null).isDeterministic(false)
                    .ownerClass(SYSTEM_PROCEDURES)
                    .varchar("BACKUPDIR", Limits.DB2_VARCHAR_MAXWIDTH)
                    .smallint("DELETE_ARCHIVED_LOG_FILES").build()
            ,
            Procedure.newBuilder().name("SYSCS_DISABLE_LOG_ARCHIVE_MODE").numOutputParams(0).numResultSets(0)
                    .sqlControl(RoutineAliasInfo.MODIFIES_SQL_DATA).returnType(null).isDeterministic(false)
                    .ownerClass(SYSTEM_PROCEDURES)
                    .smallint("DELETE_ARCHIVED_LOG_FILES").build()
            ,
            Procedure.newBuilder().name("SYSCS_SET_RUNTIMESTATISTICS").numOutputParams(0).numResultSets(0)
                    .sqlControl(RoutineAliasInfo.CONTAINS_SQL).returnType(null).isDeterministic(false)
                    .ownerClass(SYSTEM_PROCEDURES)
                    .smallint("ENABLE").build()
            ,
            Procedure.newBuilder().name("SYSCS_SET_STATISTICS_TIMING").numOutputParams(0).numResultSets(0)
                    .sqlControl(RoutineAliasInfo.CONTAINS_SQL).returnType(null).isDeterministic(false)
                    .ownerClass(SYSTEM_PROCEDURES)
                    .smallint("ENABLE").build()
            ,
            Procedure.newBuilder().name("SYSCS_GET_DATABASE_PROPERTY").numOutputParams(0).numResultSets(0)
                    .sqlControl(RoutineAliasInfo.READS_SQL_DATA)
                    .returnType(DataTypeDescriptor.getCatalogType(Types.VARCHAR,Limits.DB2_VARCHAR_MAXWIDTH))
                    .isDeterministic(false)
                    .ownerClass(SYSTEM_PROCEDURES)
                    .catalog("KEY").build()
            ,
            Procedure.newBuilder().name("SYSCS_CHECK_TABLE").numOutputParams(0).numResultSets(0)
            	.sqlControl(RoutineAliasInfo.READS_SQL_DATA)
            	.returnType(DataTypeDescriptor.getCatalogType(Types.VARCHAR,Limits.DB2_VARCHAR_MAXWIDTH))
            	.isDeterministic(false)
            	.ownerClass(SYSTEM_PROCEDURES)
            	.catalog("SCHEMA")
            	.catalog("TABLENAME").build()
            ,
            Procedure.newBuilder().name("SYSCS_GET_RUNTIMESTATISTICS").numOutputParams(0).numResultSets(0)
                    .sqlControl(RoutineAliasInfo.CONTAINS_SQL)
                    .returnType(DataTypeDescriptor.getCatalogType(Types.VARCHAR,Limits.DB2_VARCHAR_MAXWIDTH))
                    .isDeterministic(false)
                    .ownerClass(SYSTEM_PROCEDURES)
                    .build()
            ,
            Procedure.newBuilder().name("SYSCS_EXPORT_TABLE").numOutputParams(0).numResultSets(0)
                    .sqlControl(RoutineAliasInfo.READS_SQL_DATA).returnType(null).isDeterministic(false)
                    .ownerClass(SYSTEM_PROCEDURES)
                    .catalog("schemaName")
                    .catalog("tableName")
                    .varchar("fileName",32762)
                    .charType("columnDelimiter",1)
                    .charType("characterDelimiter",1)
                    .catalog("codeset").build()
            ,
            Procedure.newBuilder().name("SYSCS_EXPORT_QUERY").numOutputParams(0).numResultSets(0)
                .sqlControl(RoutineAliasInfo.READS_SQL_DATA).returnType(null).isDeterministic(false)
                .ownerClass(SYSTEM_PROCEDURES)
                .varchar("selectStatement",32762)
                .varchar("fileName",32762)
                .charType("columnDelimiter",1)
                .charType("characterDelimiter",1)
                .catalog("codeset").build()
            ,
            Procedure.newBuilder().name("SYSCS_IMPORT_TABLE").numOutputParams(0).numResultSets(0)
                .sqlControl(RoutineAliasInfo.MODIFIES_SQL_DATA).returnType(null).isDeterministic(false)
                .ownerClass(SYSTEM_PROCEDURES)
                .catalog("schemaName")
                .catalog("tableName")
                .varchar("fileName",32762)
                .charType("columnDelimiter",1)
                .charType("characterDelimiter",1)
                .catalog("codeset")
                .smallint("replace").build()
            ,
            Procedure.newBuilder().name("SYSCS_IMPORT_DATA").numOutputParams(0).numResultSets(0)
                .sqlControl(RoutineAliasInfo.MODIFIES_SQL_DATA).returnType(null).isDeterministic(false)
                .ownerClass(SYSTEM_PROCEDURES)
                .catalog("schemaName")
                .catalog("tableName")
                .varchar("insertColumnList",32762)
                .varchar("columnIndexes",32762)
                .varchar("fileName",32762)
                .varchar("columnDelimiter",32762)
                .charType("characterDelimiter",1)
                .varchar("timestampFormat",32762).build()
            ,
            Procedure.newBuilder().name("SYSCS_BULK_INSERT").numOutputParams(0).numResultSets(0)
                .sqlControl(RoutineAliasInfo.MODIFIES_SQL_DATA).returnType(null).isDeterministic(false)
                .ownerClass(SYSTEM_PROCEDURES)
                .catalog("schemaName")
                .catalog("tableName")
                .varchar("vtiName",32762)
                .varchar("vtiArg",32762).build()
            ,
            Procedure.newBuilder().name("SYSCS_INPLACE_COMPRESS_TABLE").numOutputParams(0).numResultSets(0)
                .sqlControl(RoutineAliasInfo.MODIFIES_SQL_DATA).returnType(null).isDeterministic(false)
                .ownerClass(SYSTEM_PROCEDURES)
                .catalog("SCHEMANAME")
                .catalog("TABLENAME")
                .smallint("PURGE_ROWS")
                .smallint("DEFRAGMENT_ROWS")
                .smallint("TRUNCATE_END").build()
            ,
            Procedure.newBuilder().name("SYSCS_BACKUP_DATABASE_NOWAIT").numOutputParams(0).numResultSets(0)
                .sqlControl(RoutineAliasInfo.MODIFIES_SQL_DATA).returnType(null).isDeterministic(false)
                .ownerClass(SYSTEM_PROCEDURES)
                .varchar("BACKUPDIR",Limits.DB2_VARCHAR_MAXWIDTH).build()
            ,
            Procedure.newBuilder().name("SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE_NOWAIT")
                .numOutputParams(0).numResultSets(0)
                .sqlControl(RoutineAliasInfo.MODIFIES_SQL_DATA).returnType(null).isDeterministic(false)
                .ownerClass(SYSTEM_PROCEDURES)
                .varchar("BACKUPDIR", Limits.DB2_VARCHAR_MAXWIDTH)
                .smallint("DELETEONLINEARCHIVEDLOGFILES").build()
            ,
            Procedure.newBuilder().name("SYSCS_UPDATE_STATISTICS")
                .numOutputParams(0).numResultSets(0)
                .modifiesSql().returnType(null).isDeterministic(false)
                .ownerClass(SYSTEM_PROCEDURES)
                .catalog("SCHEMANAME")
                .catalog("TABLENAME")
                .catalog("INDEXNAME")
            .build()
            ,
            Procedure.newBuilder().name("SYSCS_SET_XPLAIN_MODE")
                .numOutputParams(0).numResultSets(0)
                .containsSql().returnType(null).isDeterministic(false)
                .ownerClass(SYSTEM_PROCEDURES)
                .integer("mode")
            .build()
            ,
            Procedure.newBuilder().name("SYSCS_GET_XPLAIN_MODE")
                .numOutputParams(0).numResultSets(0)
                .readsSqlData().returnType(TypeDescriptor.INTEGER).isDeterministic(false)
                .ownerClass(SYSTEM_PROCEDURES)
                .build()
            ,
            Procedure.newBuilder().name("SYSCS_SET_XPLAIN_SCHEMA")
                .numOutputParams(0).numResultSets(0)
                .sqlControl(RoutineAliasInfo.MODIFIES_SQL_DATA).returnType(null).isDeterministic(false)
                .ownerClass(SYSTEM_PROCEDURES)
                .catalog("schemaName")
            .build()
            ,
            Procedure.newBuilder().name("SYSCS_GET_XPLAIN_SCHEMA")
                .numOutputParams(0).numResultSets(0)
                .readsSqlData()
                .returnType(DataTypeDescriptor.getBuiltInDataTypeDescriptor(
                            Types.VARCHAR,false,128).getCatalogType())
                .isDeterministic(false).ownerClass(SYSTEM_PROCEDURES)
                .build()
            ,
            Procedure.newBuilder().name("SYSCS_EXPORT_TABLE_LOBS_TO_EXTFILE")
                .numOutputParams(0).numResultSets(0)
                .readsSqlData().returnType(null).isDeterministic(false)
                .ownerClass(SYSTEM_PROCEDURES)
                .catalog("schemaName")
                .catalog("tableName")
                .varchar("fileName",32672)
                .charType("columnDelimiter",1)
                .charType("characterDelimiter",1)
                .catalog("codeset")
                .varchar("lobsFileName",32672)
            .build()
            ,
            Procedure.newBuilder().name("SYSCS_EXPORT_QUERY_LOBS_TO_EXTFILE")
                .numOutputParams(0).numResultSets(0)
                .readsSqlData().returnType(null).isDeterministic(false)
                .ownerClass(SYSTEM_PROCEDURES)
                .varchar("selectStatement",32672)
                .varchar("fileName",32672)
                .charType("columnDelimiter",1)
                .charType("characterDelimiter",1)
                .catalog("codeset")
                .varchar("lobsFileName",32672)
                .build()
            ,
            Procedure.newBuilder().name("SYSCS_IMPORT_TABLE_LOBS_FROM_EXTFILE")
                .numOutputParams(0).numResultSets(0)
                .modifiesSql().returnType(null).isDeterministic(false)
                .ownerClass(SYSTEM_PROCEDURES)
                .catalog("schemaName")
                .catalog("tableName")
                .varchar("fileName",32672)
                .charType("columnDelimiter",1)
                .charType("characterDelimiter",1)
                .catalog("codeset")
                .smallint("replace")
                .build()
            ,
            Procedure.newBuilder().name("SYSCS_IMPORT_DATA_LOBS_FROM_EXTFILE")
                .numOutputParams(0).numResultSets(0)
                .modifiesSql().returnType(null).isDeterministic(false)
                .ownerClass(SYSTEM_PROCEDURES)
                .catalog("schemaName")
                .catalog("tableName")
                .varchar("insertColumnList",32672)
                .varchar("columnIndexes",32672)
                .varchar("fileName",32672)
                .charType("columnDelimiter",1)
                .charType("characterDelimiter",1)
                .catalog("codeset")
                .smallint("replace")
                .build()
            ,
            Procedure.newBuilder().name("SYSCS_RELOAD_SECURITY_POLICY")
                .numOutputParams(0).numResultSets(0).sqlControl(RoutineAliasInfo.NO_SQL)
                .returnType(null).isDeterministic(false)
                .ownerClass(SYSTEM_PROCEDURES)
                .build()
            ,
            Procedure.newBuilder().name("SYSCS_SET_USER_ACCESS")
                .numOutputParams(0).numResultSets(0).modifiesSql()
                .returnType(null).isDeterministic(false)
                .ownerClass(SYSTEM_PROCEDURES)
                .catalog("USERNAME")
                .catalog("CONNECTIONPERMISSION")
                .build()
            ,
            Procedure.newBuilder().name("SYSCS_GET_USER_ACCESS")
                .numOutputParams(0).numResultSets(0).readsSqlData()
                .readsSqlData().returnType(CATALOG_TYPE_SYSTEM_IDENTIFIER).isDeterministic(false)
                .ownerClass(SYSTEM_PROCEDURES)
                .varchar("USERNAME",32672)
                .build()
            ,
            Procedure.newBuilder().name("SYSCS_EMPTY_STATEMENT_CACHE")
                .numOutputParams(0).numResultSets(0).sqlControl(RoutineAliasInfo.NO_SQL)
                .returnType(null).isDeterministic(false)
                .ownerClass(SYSTEM_PROCEDURES)
                .build()
            ,
            Procedure.newBuilder().name("SYSCS_CREATE_USER")
                .numOutputParams(0).numResultSets(0).modifiesSql()
                .returnType(null).isDeterministic(false)
                .ownerClass(SYSTEM_PROCEDURES)
                .catalog("userName")
                .varchar("password",32672)
                .build()
            ,
            Procedure.newBuilder().name("SYSCS_RESET_PASSWORD")
                .numResultSets(0).numOutputParams(0).modifiesSql()
                .returnType(null).isDeterministic(false)
                .ownerClass(SYSTEM_PROCEDURES)
                .catalog("userName")
                .varchar("password",32672)
                .build()
            ,
            Procedure.newBuilder().name("SYSCS_MODIFY_PASSWORD")
                .numOutputParams(0).numResultSets(0).modifiesSql()
                .returnType(null).isDeterministic(false)
                .ownerClass(SYSTEM_PROCEDURES)
                .varchar("password",32672)
                .build()
            ,
            Procedure.newBuilder().name("SYSCS_DROP_USER")
                .numOutputParams(0).numResultSets(0).modifiesSql()
                .returnType(null).isDeterministic(false)
                .ownerClass(SYSTEM_PROCEDURES)
                .catalog("userName")
                .build()
            ,
            Procedure.newBuilder().name("SYSCS_PEEK_AT_SEQUENCE")
                .numOutputParams(0).numResultSets(0).readsSqlData()
                .returnType(DataTypeDescriptor.getCatalogType(Types.BIGINT))
                .isDeterministic(false).ownerClass(SYSTEM_PROCEDURES)
                .catalog("schemaName")
                .catalog("sequenceName")
                .build()
            ,
            Procedure.newBuilder().name("SYSCS_DROP_STATISTICS")
                .numOutputParams(0).numResultSets(0).modifiesSql()
                .returnType(null).isDeterministic(false)
                .ownerClass(SYSTEM_PROCEDURES)
                .catalog("SCHEMANAME")
                .catalog("TABLENAME")
                .catalog("INDEXNAME")
                .build()
            ,
            Procedure.newBuilder().name("SYSCS_INVALIDATE_STORED_STATEMENTS")
                .numOutputParams(0).numResultSets(0).modifiesSql()
                .returnType(null).isDeterministic(false)
                .ownerClass(SYSTEM_PROCEDURES)
                .build()
                ,
            Procedure.newBuilder().name("SYSCS_UPDATE_METADATA_STORED_STATEMENTS")
                .numOutputParams(0).numResultSets(0).modifiesSql()
                .returnType(null).isDeterministic(false)
                .ownerClass(SYSTEM_PROCEDURES)
                .build()
                ,
            Procedure.newBuilder().name("SYSCS_UPDATE_SYSTEM_PROCEDURE")
                .numOutputParams(0).numResultSets(0).modifiesSql()
                .returnType(null).isDeterministic(false)
                .ownerClass(SYSTEM_PROCEDURES)
                .catalog("schemaName")
                .catalog("procName")
                .build()
                ,
            Procedure.newBuilder().name("SYSCS_UPDATE_ALL_SYSTEM_PROCEDURES")
                .numOutputParams(0).numResultSets(0).modifiesSql()
                .returnType(null).isDeterministic(false)
                .ownerClass(SYSTEM_PROCEDURES)
                .catalog("schemaName")
                .build()
    }));

    private static final List/*<Procedure>*/ sqlJProcedures = Arrays.asList(new Procedure[]{
    		Procedure.newBuilder().name("INSTALL_JAR").numOutputParams(0).numResultSets(0)
            	.sqlControl(RoutineAliasInfo.MODIFIES_SQL_DATA).returnType(null).isDeterministic(false)
            	.ownerClass(SYSTEM_PROCEDURES)
            	.varchar("URL",256)
            	.catalog("JAR")
            	.integer("DEPLOY").build()
            ,
            Procedure.newBuilder().name("REPLACE_JAR").numOutputParams(0).numResultSets(0)
                .sqlControl(RoutineAliasInfo.MODIFIES_SQL_DATA).returnType(null).isDeterministic(false)
                .ownerClass(SYSTEM_PROCEDURES)
                .varchar("URL",256)
                .catalog("JAR").build()
            ,
            Procedure.newBuilder().name("REMOVE_JAR").numOutputParams(0).numResultSets(0)
                .sqlControl(RoutineAliasInfo.MODIFIES_SQL_DATA).returnType(null).isDeterministic(false)
                .ownerClass(SYSTEM_PROCEDURES)
                .catalog("JAR")
                .integer("UNDEPLOY").build()
    });

    private static final List/*<Procedure>*/ sysIbmProcedures = Arrays.asList(new Procedure[]{
            Procedure.newBuilder().name("SQLCAMESSAGE").numOutputParams(2).numResultSets(0)
                    .sqlControl(RoutineAliasInfo.READS_SQL_DATA).returnType(null).isDeterministic(false)
                    .ownerClass(SYSTEM_PROCEDURES)
                    .integer("SQLCODE")
                    .smallint("ERRMCLEN")
                    .varchar("SQLERRMC", Limits.DB2_JCC_MAX_EXCEPTION_PARAM_LENGTH)
                    .charType("SQLERRP", 8)
                    .integer("SQLERRD0")
                    .integer("SQLERRD1")
                    .integer("SQLERRD2")
                    .integer("SQLERRD3")
                    .integer("SQLERRD4")
                    .integer("SQLERRD5")
                    .charType("SQLWARN",11)
                    .charType("SQLSTATE",5)
                    .varchar("FILE",50)
                    .charType("LOCALE",5)
                    .varchar("MESSAGE",2400)
                    .integer("RETURNCODE").build()
            ,
            Procedure.newBuilder().name("SQLPROCEDURES").numOutputParams(0).numResultSets(1)
                    .sqlControl(RoutineAliasInfo.READS_SQL_DATA).returnType(null).isDeterministic(false)
                    .ownerClass(SYSTEM_PROCEDURES)
                    .catalog("CATALOGNAME")
                    .catalog("SCHEMANAME")
                    .catalog("PROCNAME")
                    .varchar("OPTIONS",4000).build()
            ,
            Procedure.newBuilder().name("SQLTABLEPRIVILEGES").numOutputParams(0).numResultSets(1)
                    .sqlControl(RoutineAliasInfo.READS_SQL_DATA).returnType(null).isDeterministic(false)
                    .ownerClass(SYSTEM_PROCEDURES)
                    .catalog("CATALOGNAME")
                    .catalog("SCHEMANAME")
                    .catalog("TABLENAME")
                    .varchar("OPTIONS",4000).build()
            ,
            Procedure.newBuilder().name("SQLPRIMARYKEYS").numOutputParams(0).numResultSets(1)
                    .sqlControl(RoutineAliasInfo.READS_SQL_DATA).returnType(null).isDeterministic(false)
                    .ownerClass(SYSTEM_PROCEDURES)
                    .catalog("CATALOGNAME")
                    .catalog("SCHEMANAME")
                    .catalog("TABLENAME")
                    .varchar("OPTIONS",4000).build()
            ,
            Procedure.newBuilder().name("SQLTABLES").numOutputParams(0).numResultSets(1)
                    .sqlControl(RoutineAliasInfo.READS_SQL_DATA).returnType(null).isDeterministic(false)
                    .ownerClass(SYSTEM_PROCEDURES)
                    .catalog("CATALOGNAME")
                    .catalog("SCHEMANAME")
                    .catalog("TABLENAME")
                    .varchar("TABLETYPE",4000)
                    .varchar("OPTIONS",4000).build()
            ,
            Procedure.newBuilder().name("SQLPROCEDURECOLS").numOutputParams(0).numResultSets(1)
                    .sqlControl(RoutineAliasInfo.READS_SQL_DATA).returnType(null).isDeterministic(false)
                    .ownerClass(SYSTEM_PROCEDURES)
                    .catalog("CATALOGNAME")
                    .catalog("SCHEMANAME")
                    .catalog("PROCNAME")
                    .catalog("PARAMNAME")
                    .varchar("OPTIONS",4000).build()
            ,
            Procedure.newBuilder().name("SQLCOLUMNS").numOutputParams(0).numResultSets(1)
                    .sqlControl(RoutineAliasInfo.READS_SQL_DATA).returnType(null).isDeterministic(false)
                    .ownerClass(SYSTEM_PROCEDURES)
                    .catalog("CATALOGNAME")
                    .catalog("SCHEMANAME")
                    .catalog("TABLENAME")
                    .catalog("COLUMNNAME")
                    .varchar("OPTIONS",4000).build()
            ,
            Procedure.newBuilder().name("SQLCOLPRIVILEGES").numOutputParams(0).numResultSets(1)
                    .sqlControl(RoutineAliasInfo.READS_SQL_DATA).returnType(null).isDeterministic(false)
                    .ownerClass(SYSTEM_PROCEDURES)
                    .catalog("CATALOGNAME")
                    .catalog("SCHEMANAME")
                    .catalog("TABLENAME")
                    .catalog("COLUMNNAME")
                    .varchar("OPTIONS",4000).build()
            ,
            Procedure.newBuilder().name("SQLUDTS").numOutputParams(0).numResultSets(1)
                    .sqlControl(RoutineAliasInfo.READS_SQL_DATA).returnType(null).isDeterministic(false)
                    .ownerClass(SYSTEM_PROCEDURES)
                    .catalog("CATALOGNAME")
                    .catalog("SCHEMANAME")
                    .catalog("TABLENAME")
                    .catalog("TYPENAMEPATTERN")
                    .catalog("UDTTYPES")
                    .varchar("OPTIONS",4000).build()
            ,
            Procedure.newBuilder().name("SQLFOREIGNKEYS").numOutputParams(0).numResultSets(1)
                    .sqlControl(RoutineAliasInfo.READS_SQL_DATA).returnType(null).isDeterministic(false)
                    .ownerClass(SYSTEM_PROCEDURES)
                    .catalog("PKCATALOGNAME")
                    .catalog("PKSCHEMANAME")
                    .catalog("PKTABLENAME")
                    .catalog("FKCATALOGNAME")
                    .catalog("FKSCHEMANAME")
                    .catalog("FKTABLENAME")
                    .varchar("OPTIONS",4000).build()
            ,
            Procedure.newBuilder().name("SQLSPECIALCOLUMNS").numOutputParams(0).numResultSets(1)
                    .sqlControl(RoutineAliasInfo.READS_SQL_DATA).returnType(null).isDeterministic(false)
                    .ownerClass(SYSTEM_PROCEDURES)
                    .smallint("COLTYPE")
                    .catalog("CATALOGNAME")
                    .catalog("SCHEMANAME")
                    .catalog("TABLENAME" )
                    .smallint("SCOPE")
                    .smallint("NULLABLE")
                    .varchar("OPTIONS",4000).build()
            ,
            Procedure.newBuilder().name("SQLGETTYPEINFO").numOutputParams(0).numResultSets(1)
                    .sqlControl(RoutineAliasInfo.READS_SQL_DATA).returnType(null).isDeterministic(false)
                    .ownerClass(SYSTEM_PROCEDURES)
                    .smallint("DATATYPE")
                    .varchar("OPTIONS",4000).build()
            ,
            Procedure.newBuilder().name("SQLSTATISTICS").numOutputParams(0).numResultSets(1)
                    .sqlControl(RoutineAliasInfo.READS_SQL_DATA).returnType(null).isDeterministic(false)
                    .ownerClass(SYSTEM_PROCEDURES)
                    .catalog("CATALOGNAME")
                    .catalog("SCHEMANAME")
                    .catalog("TABLENAME")
                    .smallint("UNIQUE")
                    .smallint("APPROXIMATE")
                    .varchar("OPTIONS",4000).build()
            ,
            Procedure.newBuilder().name("METADATA").numOutputParams(0).numResultSets(1)
                    .sqlControl(RoutineAliasInfo.READS_SQL_DATA).returnType(null).isDeterministic(false)
                    .ownerClass(SYSTEM_PROCEDURES).build()
            ,
            Procedure.newBuilder().name("SQLFUNCTIONS").numOutputParams(0).numResultSets(1)
                .readsSqlData().returnType(null).isDeterministic(false)
                .ownerClass(SYSTEM_PROCEDURES)
                .catalog("CATALOGNAME")
                .catalog("SCHEMANAME")
                .catalog("FUNCNAME")
                .varchar("OPTIONS",4000).build()
            ,
            Procedure.newBuilder().name("SQLFUNCTIONPARAMS").numOutputParams(0).numResultSets(1)
                .readsSqlData().returnType(null).isDeterministic(false)
                .ownerClass(SYSTEM_PROCEDURES)
                .catalog("CATALOGNAME")
                .catalog("SCHEMANAME")
                .catalog("FUNCNAME")
                .catalog("PARAMNAME")
                .varchar("OPTIONS",4000).build()
            ,
            Procedure.newBuilder().name("CLOBCREATELOCATOR").numOutputParams(0).numResultSets(0)
                .containsSql().returnType(TypeDescriptor.INTEGER).isDeterministic(false)
                .ownerClass(LOB_STORED_PROCEDURE).build()
            ,
            Procedure.newBuilder().name("CLOBRELEASELOCATOR").numResultSets(0).numOutputParams(0)
                .containsSql().returnType(null).isDeterministic(false)
                .ownerClass(LOB_STORED_PROCEDURE)
                .integer("LOCATOR").build()
            ,
            Procedure.newBuilder().name("CLOBGETPROSITIONFROMSTRING").numOutputParams(0).numResultSets(0)
                .containsSql().returnType(DataTypeDescriptor.getCatalogType(Types.BIGINT))
                .isDeterministic(false).ownerClass(LOB_STORED_PROCEDURE)
                .integer("LOCATOR")
                .varchar("SEARCHSTR",Limits.DB2_VARCHAR_MAXWIDTH)
                .bigint("POS").build()
            ,
            Procedure.newBuilder().name("CLOBGETPOSITIONFROMLOCATOR").numOutputParams(0).numResultSets(0)
                .containsSql().returnType(DataTypeDescriptor.getCatalogType(Types.BIGINT))
                .isDeterministic(false).ownerClass(LOB_STORED_PROCEDURE)
                .integer("LOCATOR")
                .integer("SEARCHLOCATOR")
                .integer("POS").build()
            ,
            Procedure.newBuilder().name("CLOBGETLENGTH").numOutputParams(0).numResultSets(0)
                .containsSql().returnType(DataTypeDescriptor.getCatalogType(Types.BIGINT))
                .isDeterministic(false).ownerClass(LOB_STORED_PROCEDURE)
                .integer("LOCATOR").build()
            ,
            Procedure.newBuilder().name("CLOBGETSUBSTRING").numOutputParams(0).numResultSets(0)
                .containsSql().returnType(DataTypeDescriptor.getCatalogType(Types.VARCHAR,Limits.MAX_CLOB_RETURN_LEN))
                .isDeterministic(false).ownerClass(LOB_STORED_PROCEDURE)
                .integer("LOCATOR")
                .bigint("POS")
                .integer("LEN").build()
            ,
            Procedure.newBuilder().name("CLOBSETSTRING").numOutputParams(0).numResultSets(0)
                .containsSql().returnType(null).isDeterministic(false)
                .ownerClass(LOB_STORED_PROCEDURE)
                .integer("LOCATOR")
                .bigint("POS")
                .integer("LEN")
                .varchar("REPLACESTR",Limits.DB2_VARCHAR_MAXWIDTH).build()
            ,
            Procedure.newBuilder().name("CLOBTRUNCATE").numOutputParams(0).numResultSets(0)
                .containsSql().returnType(null).isDeterministic(false)
                .ownerClass(LOB_STORED_PROCEDURE)
                .integer("LOCATOR")
                .bigint("LEN").build()
            ,
            Procedure.newBuilder().name("BLOBCREATELOCATOR").numOutputParams(0).numResultSets(0)
                .containsSql().returnType(TypeDescriptor.INTEGER).isDeterministic(false)
                .ownerClass(LOB_STORED_PROCEDURE)
                .build()
            ,
            Procedure.newBuilder().name("BLOBRELEASELOCATOR").numOutputParams(0).numResultSets(0)
                .containsSql().returnType(null).isDeterministic(false)
                .ownerClass(LOB_STORED_PROCEDURE)
                .integer("LOCATOR")
                .build()
            ,
            Procedure.newBuilder().name("BLOBGETPOSITIONFROMBYTES").numOutputParams(0).numResultSets(0)
                .containsSql().returnType(DataTypeDescriptor.getCatalogType(Types.BIGINT))
            .ownerClass(LOB_STORED_PROCEDURE).isDeterministic(false)
                .integer("LOCATOR")
                .arg("SEARCHBYTES",DataTypeDescriptor.getCatalogType(Types.VARBINARY))
                .bigint("POS")
                .build()
            ,
            Procedure.newBuilder().name("BLOBGETPOSITIONFROMLOCATOR").numOutputParams(0).numResultSets(0)
                .containsSql().returnType(DataTypeDescriptor.getCatalogType(Types.BIGINT))
                .isDeterministic(false).ownerClass(LOB_STORED_PROCEDURE)
                .integer("LOCATOR")
                .integer("SEARCHLOCATOR")
                .bigint("POS")
                .build()
            ,
            Procedure.newBuilder().name("BLOBGETLENGTH").numOutputParams(0).numResultSets(0)
                .containsSql().returnType(DataTypeDescriptor.getCatalogType(Types.BIGINT))
                .isDeterministic(false).ownerClass(LOB_STORED_PROCEDURE)
                .integer("LOCATOR")
                .build()
            ,
            Procedure.newBuilder().name("BLOBGETBYTES").numOutputParams(0).numResultSets(0)
                .containsSql().returnType(DataTypeDescriptor.getCatalogType(Types.VARBINARY,Limits.MAX_BLOB_RETURN_LEN))
                .isDeterministic(false).ownerClass(LOB_STORED_PROCEDURE)
                .integer("LOCATOR")
                .bigint("POS")
                .integer("LEN")
                .build()
            ,
            Procedure.newBuilder().name("BLOBSETBYTES").numOutputParams(0).numResultSets(0)
                .containsSql().returnType(null).isDeterministic(false)
                .ownerClass(LOB_STORED_PROCEDURE)
                .integer("LOCATOR")
                .bigint("POS")
                .integer("LEN")
                .arg("REPLACEBYTES",DataTypeDescriptor.getCatalogType(Types.VARBINARY))
                .build()
            ,
            Procedure.newBuilder().name("BLOBTRUNCATE").numOutputParams(0).numResultSets(0)
                .containsSql().returnType(null).isDeterministic(false)
                .ownerClass(LOB_STORED_PROCEDURE)
                .integer("LOCATOR")
                .bigint("POS")
                .build()

    });
}
