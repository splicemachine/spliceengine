package com.splicemachine.derby.impl.sql.catalog;

import com.splicemachine.derby.iapi.catalog.PhysicalStatsDescriptor;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.iapi.sql.dictionary.CatalogRowFactory;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.types.*;
import org.apache.derby.impl.sql.catalog.SystemColumnImpl;

import java.sql.Types;

/**
 * @author Scott Fines
 *         Date: 2/25/15
 */
public class SYSPHYSICALSTATISTICSRowFactory extends CatalogRowFactory {
    public static final String TABLENAME_STRING = "SYSPHYSICALSTATS";
    private static final int COLUMN_COUNT = 7;
    private static final int HOSTNAME           = 1;
    private static final int NUMCPUS            = 2;
    private static final int MAX_HEAP           = 3;
    private static final int NETWORK_SIZE       = 4;
    private static final int LOCALREADLATENCY   = 5;
    private static final int REMOTEREADLATENCY  = 6;
    private static final int WRITELATENCY       = 7;

    private String[] uuids = {
            "08264013-014b-c29c-947b-000003009390",
            "08264013-014b-c29c-947b-000003009390"
    };
    public SYSPHYSICALSTATISTICSRowFactory(UUIDFactory uuidFactory, ExecutionFactory exFactory, DataValueFactory dvf) {
        super(uuidFactory,exFactory,dvf);
        initInfo(COLUMN_COUNT,TABLENAME_STRING,null,null,uuids);
    }

    @Override
    public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent) throws StandardException {
        String hostName = null;
        int numCpus = 0;
        long maxHeap = 0;
        int numIpc = 0;
        long localReadLatency = 0;
        long remoteReadLatency = 0;
        long writeLatency = 0;

        if(td!=null){
            PhysicalStatsDescriptor psd = (PhysicalStatsDescriptor)td;
            hostName = psd.getHostName();
            numCpus = psd.getNumCores();
            maxHeap = psd.getHeapSize();
            numIpc = psd.getNumIpcThreads();
            localReadLatency = psd.getLocalReadLatency();
            remoteReadLatency = psd.getRemoteReadLatency();
            writeLatency = psd.getWriteLatency();
        }

        ExecRow row = getExecutionFactory().getValueRow(COLUMN_COUNT);
        row.setColumn(HOSTNAME,new SQLVarchar(hostName));
        row.setColumn(NUMCPUS,new SQLInteger(numCpus));
        row.setColumn(MAX_HEAP,new SQLLongint(maxHeap));
        row.setColumn(NETWORK_SIZE,new SQLInteger(numIpc));
        row.setColumn(LOCALREADLATENCY,new SQLLongint(localReadLatency));
        row.setColumn(REMOTEREADLATENCY,new SQLLongint(remoteReadLatency));
        row.setColumn(WRITELATENCY,new SQLLongint(writeLatency));
        return row;
    }

    @Override
    public TupleDescriptor buildDescriptor(ExecRow row, TupleDescriptor parentTuple, DataDictionary dataDictionary) throws StandardException {

        DataValueDescriptor col = row.getColumn(HOSTNAME);
        String hostName = col.getString();
        col = row.getColumn(NUMCPUS);
        int numCores = col.getInt();
        col = row.getColumn(MAX_HEAP);
        long heapSize = col.getLong();
        col = row.getColumn(NETWORK_SIZE);
        int numIpcThreads = col.getInt();
        col = row.getColumn(LOCALREADLATENCY);
        long localReadLatency = col.getLong();
        col = row.getColumn(REMOTEREADLATENCY);
        long remoteReadLatency = col.getLong();
        col = row.getColumn(WRITELATENCY);
        long writeLatency = col.getLong();

        return new PhysicalStatsDescriptor(
                hostName,
                numCores,
                heapSize,
                numIpcThreads,
                localReadLatency,
                remoteReadLatency,
                writeLatency);

    }

    @Override
    public SystemColumn[] buildColumnList() throws StandardException {
        return new SystemColumn[] {
                SystemColumnImpl.getColumn("HOSTNAME", Types.VARCHAR,false),
                SystemColumnImpl.getColumn("NUM_CPUS", Types.INTEGER,true),
                SystemColumnImpl.getColumn("MAX_HEAP", Types.BIGINT,true),
                SystemColumnImpl.getColumn("NETWORK_CONNS", Types.INTEGER,true),
                SystemColumnImpl.getColumn("LOCAL_READ_LATENCY", Types.BIGINT,true),
                SystemColumnImpl.getColumn("REMOTE_READ_LATENCY", Types.BIGINT,true),
                SystemColumnImpl.getColumn("WRITE_LATENCY", Types.BIGINT,true),
        };
    }
}
