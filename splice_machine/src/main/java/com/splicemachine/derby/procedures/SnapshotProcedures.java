package com.splicemachine.derby.procedures;

import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.conn.ConnectionUtil;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SnapshotDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.jdbc.EmbedDatabaseMetaData;
import com.splicemachine.db.impl.sql.catalog.Procedure;
import com.splicemachine.derby.utils.DataDictionaryUtils;
import com.splicemachine.derby.utils.EngineUtils;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import splice.com.google.common.collect.Lists;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static com.splicemachine.db.shared.common.reference.SQLState.LANG_NOT_ALLOWED_FOR_TEMP_TABLE;

public class SnapshotProcedures extends BaseAdminProcedures {
    private static Logger LOG=Logger.getLogger(SnapshotProcedures.class);

    public static void addProcedures(List<Procedure> procedures) {
        Procedure snapshotSchema = Procedure.newBuilder().name("SNAPSHOT_SCHEMA")
                .varchar("schemaName", 128)
                .varchar("snapshotName", 128)
                .numOutputParams(0)
                .numResultSets(0)
                .ownerClass(SnapshotProcedures.class.getCanonicalName())
                .build();
        procedures.add(snapshotSchema);

        Procedure snapshotTable = Procedure.newBuilder().name("SNAPSHOT_TABLE")
                .varchar("schemaName", 128)
                .varchar("tableName", 128)
                .varchar("snapshotName", 128)
                .numOutputParams(0)
                .numResultSets(0)
                .ownerClass(SnapshotProcedures.class.getCanonicalName())
                .build();
        procedures.add(snapshotTable);

        Procedure deleteSnapshot = Procedure.newBuilder().name("DELETE_SNAPSHOT")
                .varchar("snapshotName", 128)
                .numOutputParams(0)
                .numResultSets(0)
                .ownerClass(SnapshotProcedures.class.getCanonicalName())
                .build();
        procedures.add(deleteSnapshot);

        Procedure restoreSnapshot = Procedure.newBuilder().name("RESTORE_SNAPSHOT")
                .varchar("snapshotName", 128)
                .numOutputParams(0)
                .numResultSets(0)
                .ownerClass(SnapshotProcedures.class.getCanonicalName())
                .build();
        procedures.add(restoreSnapshot);
    }

    /**
     * Take a snapshot of a schema
     * @param schemaName
     * @param snapshotName
     * @throws Exception
     */
    public static void SNAPSHOT_SCHEMA(String schemaName, String snapshotName) throws Exception
    {
        ensureSnapshot(snapshotName, false);
        LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
        TransactionController tc  = lcc.getTransactionExecute();
        DataDictionary dd = lcc.getDataDictionary();

        schemaName = EngineUtils.validateSchema(schemaName);
        EngineUtils.checkSchemaVisibility(schemaName);

        dd.startWriting(lcc);


        List<String> snapshotList = Lists.newArrayList();
        try
        {
            ResultSet rs = getTablesForSnapshot(schemaName, null);
            snapshot(rs, snapshotName, schemaName, dd, tc, snapshotList);
        }
        catch (Exception e)
        {
            deleteSnapshots(snapshotList);
            throw e;
        }
    }

    public static void SNAPSHOT_TABLE(String schemaName, String tableName, String snapshotName) throws Exception
    {
        ensureSnapshot(snapshotName, false);
        LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
        TransactionController tc  = lcc.getTransactionExecute();
        DataDictionary dd = lcc.getDataDictionary();

        schemaName = EngineUtils.validateSchema(schemaName);
        tableName = EngineUtils.validateTable(tableName);
        EngineUtils.checkSchemaVisibility(schemaName);

        EngineUtils.checkSchemaVisibility(schemaName);

        TableDescriptor td = DataDictionaryUtils.getTableDescriptor(lcc, schemaName, tableName);
        if (td.isExternal())
            throw StandardException.newException(SQLState.SNAPSHOT_EXTERNAL_TABLE_UNSUPPORTED, tableName);
        if (td.isTemporary())
            throw StandardException.newException(LANG_NOT_ALLOWED_FOR_TEMP_TABLE, tableName);

        List<String> snapshotList = Lists.newArrayList();
        try {
            dd.startWriting(lcc);

            ResultSet rs = getTablesForSnapshot(schemaName, td.getName());
            snapshot(rs, snapshotName, schemaName, dd, tc, snapshotList);
        }
        catch (Exception e)
        {
            deleteSnapshots(snapshotList);
            throw e;
        }
    }

    /**
     * delete a snapshot
     * @param snapshotName
     * @throws Exception
     */
    public static void DELETE_SNAPSHOT(String snapshotName) throws Exception
    {
        ensureSnapshot(snapshotName, true);
        LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
        TransactionController tc  = lcc.getTransactionExecute();
        DataDictionary dd = lcc.getDataDictionary();
        dd.startWriting(lcc);

        PartitionAdmin admin=SIDriver.driver().getTableFactory().getAdmin();
        Connection connection = getDefaultConn();
        EmbedDatabaseMetaData dmd = (EmbedDatabaseMetaData)connection.getMetaData();
        try (ResultSet rs = dmd.checkSnapshotExists(snapshotName)) {

            while (rs.next()) {
                long conglomerateNumber = rs.getLong(3);
                String sname = snapshotName + "_" + conglomerateNumber;
                if (LOG.isDebugEnabled()) {
                    SpliceLogUtils.debug(LOG, "deleting snapshot %s for table %d", sname, conglomerateNumber);
                }
                admin.deleteSnapshot(sname);
                dd.deleteSnapshot(snapshotName, conglomerateNumber, tc);

                if (LOG.isDebugEnabled()) {
                    SpliceLogUtils.debug(LOG, "deleted snapshot %s for table %d", sname, conglomerateNumber);
                }
            }
        }
    }

    /**
     * restore a snapshot
     * @param snapshotName
     * @throws Exception
     */
    public static void RESTORE_SNAPSHOT(String snapshotName) throws Exception
    {
        ensureSnapshot(snapshotName, true);
        LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
        TransactionController tc  = lcc.getTransactionExecute();
        DataDictionary dd = lcc.getDataDictionary();
        dd.startWriting(lcc);

        PartitionAdmin admin=SIDriver.driver().getTableFactory().getAdmin();
        Connection connection = getDefaultConn();
        EmbedDatabaseMetaData dmd = (EmbedDatabaseMetaData)connection.getMetaData();
        try (ResultSet rs = dmd.checkSnapshotExists(snapshotName)) {

            while (rs.next()) {
                String schemaName = rs.getString(1);
                String objectName = rs.getString(2);
                long conglomerateNumber = rs.getLong(3);
                DateTime creationTime = new DateTime(rs.getTimestamp(4));
                DateTime lastRestoreTime = new DateTime(System.currentTimeMillis());
                String sname = snapshotName + "_" + conglomerateNumber;

                if (LOG.isDebugEnabled()) {
                    SpliceLogUtils.debug(LOG, "restoring snapshot %s for table %d", sname, conglomerateNumber);
                }

                admin.disableTable(Long.toString(conglomerateNumber));
                admin.restoreSnapshot(sname);
                admin.enableTable(Long.toString(conglomerateNumber));
                dd.deleteSnapshot(snapshotName, conglomerateNumber, tc);
                SnapshotDescriptor descriptor = new SnapshotDescriptor(snapshotName, schemaName, objectName,
                        conglomerateNumber, creationTime, lastRestoreTime);
                dd.addSnapshot(descriptor, tc);
                if (LOG.isDebugEnabled()) {
                    SpliceLogUtils.debug(LOG, "restored snapshot %s for table %d", sname, conglomerateNumber);
                }
            }
        }
    }

    private static ResultSet getTablesForSnapshot(String schemaName, String tableName) throws Exception
    {
        EmbedConnection defaultConn=(EmbedConnection)getDefaultConn();
        EmbedDatabaseMetaData dmd = (EmbedDatabaseMetaData)defaultConn.getMetaData();
        ResultSet rs = dmd.getTablesForSnaphot(schemaName, tableName);
        return rs;
    }

    private static void snapshot(ResultSet rs, String snapshotName, String schemaName,
                                 DataDictionary dd, TransactionController tc, List<String> snapshotList) throws Exception
    {
        PartitionAdmin admin=SIDriver.driver().getTableFactory().getAdmin();
        while(rs.next())
        {
            String objectName = rs.getString(1);
            long conglomerateNumber = rs.getLong(2);
            String sname = snapshotName + "_" + conglomerateNumber;
            DateTime creationTime = new DateTime(System.currentTimeMillis());
            if (LOG.isDebugEnabled())
            {
                SpliceLogUtils.debug(LOG, "creating snapshot %s", sname);
            }
            SnapshotDescriptor descriptor =
                    new SnapshotDescriptor(snapshotName, schemaName, objectName, conglomerateNumber,creationTime, null);
            admin.snapshot(sname, Long.toString(conglomerateNumber));
            dd.addSnapshot(descriptor, tc);
            snapshotList.add(sname);
            if (LOG.isDebugEnabled())
            {
                SpliceLogUtils.debug(LOG, "created snapshot %s", sname);
            }
        }
    }

    private static void ensureSnapshot(String snapshotName, boolean exists) throws StandardException
    {

        if (!snapshotName.matches("[a-zA-Z_0-9][a-zA-Z_0-9-.]*")) {
            throw StandardException.newException(SQLState.SNAPSHOT_NAME_ILLEGAL,snapshotName);
        }
        int count = 0;
        try {
            Connection connection = getDefaultConn();
            EmbedDatabaseMetaData dmd = (EmbedDatabaseMetaData)connection.getMetaData();
            try (ResultSet rs = dmd.checkSnapshotExists(snapshotName)) {
                if (rs.next()) {
                    count++;
                }
            }
        }
        catch (SQLException e)
        {
            throw StandardException.plainWrapException(e);
        }

        if (exists && count == 0)
        {
            throw StandardException.newException(SQLState.SNAPSHOT_NOT_EXISTS, snapshotName);
        }
        else if (!exists && count > 0)
        {
            throw StandardException.newException(SQLState.SNAPSHOT_EXISTS, snapshotName);
        }
    }

    private static void deleteSnapshots(List<String> snapshotList) throws IOException
    {
        PartitionAdmin admin=SIDriver.driver().getTableFactory().getAdmin();
        for (String snapshot : snapshotList)
        {
            admin.deleteSnapshot(snapshot);
        }
    }
}
