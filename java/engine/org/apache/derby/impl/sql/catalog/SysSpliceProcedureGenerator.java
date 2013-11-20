package org.apache.derby.impl.sql.catalog;

import org.apache.derby.catalog.UUID;
import org.apache.derby.catalog.types.RoutineAliasInfo;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.Limits;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.store.access.TransactionController;

import java.util.HashSet;

/**
 * @author Scott Fines
 *         Created on: 2/18/13
 */
public class SysSpliceProcedureGenerator implements SystemProcedureGenerator {
    private final DataDictionary dictionary;

    public SysSpliceProcedureGenerator(DataDictionary dictionary) {
        this.dictionary = dictionary;
    }

    public void createProcedures(TransactionController tc, HashSet newlyCreatedRoutines) throws StandardException {
        UUID SYSSPLICEUUID = dictionary.getSysSpliceSchemaDescriptor().getUUID();

        for(int i=0;i<procedures.length;i++){
            newlyCreatedRoutines.add(procedures[i].createSystemProcedure(SYSSPLICEUUID,dictionary,tc));
        }
    }

    private static final Procedure[] procedures = new Procedure[]{
            Procedure.newBuilder().name("SQLCAMESSAGE").numOutputParams(2).numResultSets(0)
                    .sqlControl(RoutineAliasInfo.READS_SQL_DATA).returnType(null).isDeterministic(false)
                    .ownerClass("org.apache.derby.catalog.SystemProcedures")
                    .integer("SQLCODE")
                    .smallint("SQLERRML")
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
                    .ownerClass("org.apache.derby.catalog.SystemProcedures")
                    .catalog("CATALOGNAME")
                    .catalog("SCHEMANAME")
                    .catalog("PROCNAME")
                    .varchar("OPTIONS",4000).build()
            ,
            Procedure.newBuilder().name("SQLTABLEPRIVILEGES").numOutputParams(0).numResultSets(1)
                    .sqlControl(RoutineAliasInfo.READS_SQL_DATA).returnType(null).isDeterministic(false)
                    .ownerClass("org.apache.derby.catalog.SystemProcedures")
                    .catalog("CATALOGNAME")
                    .catalog("SCHEMANAME")
                    .catalog("TABLENAME")
                    .varchar("OPTIONS",4000).build()
            ,
            Procedure.newBuilder().name("SQLPRIMARYKEYS").numOutputParams(0).numResultSets(1)
                    .sqlControl(RoutineAliasInfo.READS_SQL_DATA).returnType(null).isDeterministic(false)
                    .ownerClass("org.apache.derby.catalog.SystemProcedures")
                    .catalog("CATALOGNAME")
                    .catalog("SCHEMANAME")
                    .catalog("TABLENAME")
                    .varchar("OPTIONS",4000).build()
            ,
            Procedure.newBuilder().name("SQLTABLES").numOutputParams(0).numResultSets(1)
                    .sqlControl(RoutineAliasInfo.READS_SQL_DATA).returnType(null).isDeterministic(false)
                    .ownerClass("org.apache.derby.catalog.SystemProcedures")
                    .catalog("CATALOGNAME")
                    .catalog("SCHEMANAME")
                    .catalog("TABLENAME")
                    .varchar("TABLETYPE",4000)
                    .varchar("OPTIONS",4000).build()
            ,
            Procedure.newBuilder().name("SQLPROCEDURECOLS").numOutputParams(0).numResultSets(1)
                    .sqlControl(RoutineAliasInfo.READS_SQL_DATA).returnType(null).isDeterministic(false)
                    .ownerClass("org.apache.derby.catalog.SystemProcedures")
                    .catalog("CATALOGNAME")
                    .catalog("SCHEMANAME")
                    .catalog("PROCNAME")
                    .catalog("PARAMNAME")
                    .varchar("OPTIONS",4000).build()
            ,
            Procedure.newBuilder().name("SQLCOLUMNS").numOutputParams(0).numResultSets(1)
                    .sqlControl(RoutineAliasInfo.READS_SQL_DATA).returnType(null).isDeterministic(false)
                    .ownerClass("org.apache.derby.catalog.SystemProcedures")
                    .catalog("CATALOGNAME")
                    .catalog("SCHEMANAME")
                    .catalog("TABLENAME")
                    .catalog("COLUMNNAME")
                    .varchar("OPTIONS",4000).build()
            ,
            Procedure.newBuilder().name("SQLCOLPRIVILEGES").numOutputParams(0).numResultSets(1)
                    .sqlControl(RoutineAliasInfo.READS_SQL_DATA).returnType(null).isDeterministic(false)
                    .ownerClass("org.apache.derby.catalog.SystemProcedures")
                    .catalog("CATALOGNAME")
                    .catalog("SCHEMANAME")
                    .catalog("TABLENAME")
                    .catalog("COLUMNNAME")
                    .varchar("OPTIONS",4000).build()
            ,
            Procedure.newBuilder().name("SQLUDTS").numOutputParams(0).numResultSets(1)
                    .sqlControl(RoutineAliasInfo.READS_SQL_DATA).returnType(null).isDeterministic(false)
                    .ownerClass("org.apache.derby.catalog.SystemProcedures")
                    .catalog("CATALOGNAME")
                    .catalog("SCHEMANAME")
                    .catalog("TABLENAME")
                    .catalog("TYPENAMEPATTERN")
                    .catalog("UDTTYPES")
                    .varchar("OPTIONS",4000).build()
            ,
            Procedure.newBuilder().name("SQLFOREIGNKEYS").numOutputParams(0).numResultSets(1)
                    .sqlControl(RoutineAliasInfo.READS_SQL_DATA).returnType(null).isDeterministic(false)
                    .ownerClass("org.apache.derby.catalog.SystemProcedures")
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
                    .ownerClass("org.apache.derby.catalog.SystemProcedures")
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
                    .ownerClass("org.apache.derby.catalog.SystemProcedures")
                    .smallint("DATATYPE")
                    .varchar("OPTIONS",4000).build()
            ,
            Procedure.newBuilder().name("SQLSTATISTICS").numOutputParams(0).numResultSets(1)
                    .sqlControl(RoutineAliasInfo.READS_SQL_DATA).returnType(null).isDeterministic(false)
                    .ownerClass("org.apache.derby.catalog.SystemProcedures")
                    .catalog("CATALOGNAME")
                    .catalog("SCHEMANAME")
                    .catalog("TABLENAME")
                    .smallint("UNIQUE")
                    .smallint("RESERVED")
                    .varchar("OPTIONS",4000).build()
            ,
            Procedure.newBuilder().name("METADATA").numOutputParams(0).numResultSets(1)
                    .sqlControl(RoutineAliasInfo.READS_SQL_DATA).returnType(null).isDeterministic(false)
                    .ownerClass("org.apache.derby.catalog.SystemProcedures").build()
    };

}
