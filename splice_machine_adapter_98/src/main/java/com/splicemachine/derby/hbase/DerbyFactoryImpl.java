package com.splicemachine.derby.hbase;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicReference;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import com.splicemachine.access.hbase.HBaseTableFactory;
import com.splicemachine.access.hbase.HBaseTableInfoFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.log4j.Logger;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.iapi.types.SQLLongint;
import com.splicemachine.db.iapi.types.SQLReal;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.jdbc.EmbedResultSet;
import com.splicemachine.db.impl.jdbc.EmbedResultSet40;
import com.splicemachine.db.impl.sql.GenericColumnDescriptor;
import com.splicemachine.db.impl.sql.execute.IteratorNoPutResultSet;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.job.scheduler.SubregionSplitter;
import com.splicemachine.derby.impl.sql.execute.DropIndexConstantOperation;
import com.splicemachine.derby.impl.sql.execute.operations.SkippingScanFilter;
import com.splicemachine.derby.impl.store.access.base.SpliceGenericCostController;
import com.splicemachine.derby.utils.BaseAdminProcedures;
import com.splicemachine.derby.utils.SpliceAdmin;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.hbase.HBaseRegionLoads;
import com.splicemachine.hbase.HBaseServerUtils;
import com.splicemachine.hbase.debug.HBaseEntryPredicateFilter;
import com.splicemachine.hbase.jmx.JMXUtils;
import com.splicemachine.hbase.table.SpliceRpcController;
import com.splicemachine.mrio.api.core.MemStoreFlushAwareScanner;
import com.splicemachine.mrio.api.core.MemstoreAware;
import com.splicemachine.mrio.api.core.SMSplit;
import com.splicemachine.mrio.api.core.SpliceRegionScanner;
import com.splicemachine.mrio.api.core.SplitRegionScanner;
import com.splicemachine.pipeline.api.BulkWritesInvoker.Factory;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.pipeline.impl.BulkWritesRPCInvoker;
import com.splicemachine.si.coprocessor.TxnMessage;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.utils.SpliceLogUtils;

public class DerbyFactoryImpl implements DerbyFactory<TxnMessage.TxnInfo> {

    protected static final String REGION_SERVER_STATISTICS = "Hadoop:service=HBase,name=RegionServer,sub=Server";

    @Override
	public Filter getAllocatedFilter(byte[] localAddress) {
		return new AllocatedFilter(localAddress);
	}

	@Override
	public List<HRegion> getOnlineRegions(RegionServerServices services,
			byte[] tableName) throws IOException {
		try {
		return services.getOnlineRegions(HBaseTableInfoFactory.getInstance().getTableInfo(tableName));
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public void removeTableFromDescriptors(MasterServices masterServices,
			String tableName) {
		try {
	      masterServices.getTableDescriptors().remove(HBaseTableInfoFactory.getInstance().getTableInfo(tableName));
		} catch (Exception e) {
			new RuntimeException(e);
		}
	}
	
	@Override
	public HRegionInfo loadRegionInfoFileContent(FileSystem fileSystem,
			Path path) throws IOException {
		return HRegionFileSystem.loadRegionInfoFileContent(fileSystem, path);
	}
	
	@Override
	public void writeScanExternal(ObjectOutput output, Scan scan)
			throws IOException {
			byte[] bytes = ProtobufUtil.toScan(scan).toByteArray();
			output.writeInt(bytes.length);
			output.write(bytes);		
	}

	@Override
	public Scan readScanExternal(ObjectInput in) throws IOException {
		byte[] scanBytes = new byte[in.readInt()];
		in.readFully(scanBytes);
		ClientProtos.Scan scan1 = ClientProtos.Scan.parseFrom(scanBytes);
		return ProtobufUtil.toScan(scan1);
	}

	
	@Override
	public void checkCallerDisconnect(HRegion region) throws IOException {
		HBaseServerUtils.checkCallerDisconnect(region, "RegionWrite");
	}
	
	@Override
	public InternalScanner noOpInternalScanner() {
		return new NoOpInternalScanner();
	}
	@Override
	public void writeRegioninfoOnFilesystem(HRegionInfo regionInfo,
			Path tabledir, FileSystem fs, Configuration conf)
			throws IOException {
		HRegionFileSystem.createRegionOnFileSystem(conf, fs, tabledir, regionInfo);
	}
	@Override
	public Path getTableDir(HRegion region) {
		return region.getRegionFileSystem().getTableDir();
	}
    @Override
    public Path getRegionDir(HRegion region) {
        return region.getRegionFileSystem().getRegionDir();
    }
	@Override
	public void bulkLoadHFiles(HRegion region, List<Pair<byte[], String>> paths) throws IOException {
		region.bulkLoadHFiles(paths,true); // TODO
	}



	@Override
	public Factory getBulkWritesInvoker(byte[] tableName) {
		return new BulkWritesRPCInvoker.Factory(tableName);
	}
	@Override
	public long computeRowCount(Logger LOG,String tableName,
			SortedSet<Pair<HRegionInfo, ServerName>> baseRegions, Scan scan) {		
	    Map<String,RegionLoad> baseRegionLoads = HBaseRegionLoads.getCachedRegionLoadsMapForTable(tableName);
		return internalComputeRowCount(LOG,baseRegions,baseRegionLoads,SpliceConstants.hbaseRegionRowEstimate,SpliceConstants.regionMaxFileSize,scan); 
	}
	
	   protected static long internalComputeRowCount(Logger LOG, SortedSet<Pair<HRegionInfo,ServerName>> regions, Map<String,RegionLoad> regionLoads, long constantRowSize, long hfileMaxSize, Scan scan) {
			long rowCount = 0;
			int numberOfRegionsInvolved = 0;
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "computeRowCount {regions={%s}, regionLoad={%s}, constantRowSize=%d, hfileMaxSize=%d, scan={%s}",
						regions==null?"null":Arrays.toString(regions.toArray()), regionLoads==null?"null":Arrays.toString(regionLoads.keySet().toArray()), constantRowSize, hfileMaxSize, scan);		
			for (Pair<HRegionInfo,ServerName> info: regions) {
				if (SpliceGenericCostController.isRegionInScan(scan,info.getFirst())) {
					if (LOG.isTraceEnabled())
						SpliceLogUtils.trace(LOG, "regionInfo with encodedname {%s} and region name as string %s", info.getFirst().getEncodedName(), info.getFirst().getRegionNameAsString());				
					numberOfRegionsInvolved++;
					rowCount+=getRowSize(LOG,constantRowSize,regionLoads==null?null:regionLoads.get(info.getFirst().getRegionNameAsString()),hfileMaxSize);	
				}
			}
			if (numberOfRegionsInvolved == 1 && scan.getStartRow() != null && !Bytes.equals(scan.getStartRow(),HConstants.EMPTY_START_ROW) && scan.getStopRow() != null && !Bytes.equals(scan.getStopRow(),HConstants.EMPTY_END_ROW) ) {
				rowCount=(long) ( ( (double)rowCount)*SpliceConstants.extraStartStopQualifierMultiplier);
			}
			return rowCount;
		}
		public static long getRowSize(Logger LOG, long constantRowSize, RegionLoad regionLoad, long hfileMaxSize) {
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "getRowSize with constantRowSize %d and regionLoad %s and hfileMaxSize %d",constantRowSize, regionLoad, hfileMaxSize);
			if (regionLoad==null)
				return constantRowSize; // No Metrics
			float rowSize = (float) constantRowSize*((float) HBaseRegionLoads.memstoreAndStorefileSize(regionLoad)/(float) hfileMaxSize);
			return rowSize < SpliceConstants.optimizerTableMinimalRows?SpliceConstants.optimizerTableMinimalRows:(long) rowSize;
		}
		@Override
		public void setMaxCardinalityBasedOnRegionLoad(String tableName,
				LanguageConnectionContext lcc) {
			  Collection<RegionLoad> regionLoads =
		              HBaseRegionLoads
		                     .getCachedRegionLoadsForTable(tableName);
		      if (regionLoads != null
		              && regionLoads.size() > lcc.getStatementContext().getMaxCardinality()){
		          lcc.getStatementContext().setMaxCardinality(regionLoads.size());
		      }			
		}

		@Override
		public int getRegionsSizeMB(String tableName) {
			int regionSizeMB = -1;
			
			Collection<RegionLoad> loads =
		            HBaseRegionLoads.getCachedRegionLoadsForTable(tableName);
		        if (loads != null && loads.size() == 1) {
		            regionSizeMB = HBaseRegionLoads
		                               .memstoreAndStorefileSize(loads.iterator().next());
		        }
		    return regionSizeMB;
		}
		@Override
		public Filter getHBaseEntryPredicateFilter(EntryPredicateFilter epf) {
			return new HBaseEntryPredicateFilter(epf);
		}
		
		@Override
		public Filter getSkippingScanFilter(
				List<Pair<byte[], byte[]>> startStopKeys,
				List<byte[]> predicates) {
			return new SkippingScanFilter(startStopKeys,predicates);
		}
		
		@Override
		public Table getTable(RegionCoprocessorEnvironment rce,
				byte[] tableName) throws IOException {
			return rce.getTable(HBaseTableInfoFactory.getInstance().getTableInfo(tableName));
		}
		@Override
		public int getReduceNumberOfRegions(String tableName, Configuration conf) throws IOException {
			try(HTable t = (HTable) HBaseTableFactory.getInstance().getTable(tableName)) {
				return t.getAllRegionLocations().size();
			}
		}

		@Override
		public ConstantAction getDropIndexConstantAction(String fullIndexName,
				String indexName, String tableName, String schemaName,
				UUID tableId, long tableConglomerateId) {
			return new DropIndexConstantOperation(fullIndexName,indexName,tableName,schemaName,tableId,tableConglomerateId);
		}
		
	    private static Map<ServerName, ServerLoad> getLoad() throws SQLException {
	        Map<ServerName, ServerLoad> serverLoadMap = new HashMap<ServerName, ServerLoad>();
	        HBaseAdmin admin = null;
	        try {
	            admin = SpliceUtils.getAdmin();
	            for (ServerName serverName : SpliceUtils.getServers()) {
	                try {
	                    serverLoadMap.put(serverName, admin.getClusterStatus().getLoad(serverName));
	                } catch (IOException e) {
	                    throw new SQLException(e);
	                }
	            }
	        } finally {
	            if (admin != null)
	                try {
	                    admin.close();
	                } catch (IOException e) {
	                    // ignore
	                }
	        }
	        return serverLoadMap;
	    }
	    
	    private static Map<String, RegionLoad> getRegionLoad() throws SQLException {
	        Map<String, RegionLoad> regionLoads = new HashMap<String, RegionLoad>();
	        HBaseAdmin admin = null;
	        admin = SpliceUtils.getAdmin();
	        try {
	            ClusterStatus clusterStatus = admin.getClusterStatus();
	            for (ServerName serverName : clusterStatus.getServers()) {
	                final ServerLoad serverLoad = clusterStatus.getLoad(serverName);

	                for (Map.Entry<byte[], RegionLoad> entry : serverLoad.getRegionsLoad().entrySet()) {
	                    regionLoads.put(Bytes.toString(entry.getKey()), entry.getValue());
	                }
	            }
	        } catch (IOException e) {
	            throw new SQLException(e);
	        } finally {
	            if (admin != null)
	                try {
	                    admin.close();
	                } catch (IOException e) {
	                    // ignore
	                }
	        }

	        return regionLoads;
	    }

		@Override
		public void SYSCS_GET_REQUESTS(ResultSet[] resultSet)
				throws SQLException {
			StringBuilder sb = new StringBuilder("select * from (values ");
	        int i = 0;
	        for (Map.Entry<ServerName, ServerLoad> serverLoad : getLoad().entrySet()) {
	            if (i != 0) {
	                sb.append(", ");
	            }
	            ServerName sn = serverLoad.getKey();
	            sb.append(String.format("('%s',%d,%d)",
	                    sn.getHostname(),
	                    sn.getPort(),
	                    serverLoad.getValue().getTotalNumberOfRequests()));
	            i++;
	        }
	        sb.append(") foo (hostname, port, totalRequests)");
	        resultSet[0] = SpliceAdmin.executeStatement(sb);	
		}

		@Override
		public void SYSCS_GET_SCHEMA_INFO(ResultSet[] resultSet)
				throws SQLException {
			ResultSet allTablesInSchema = SpliceAdmin.getDefaultConn().prepareStatement("SELECT S.SCHEMANAME, T.TABLENAME, C.ISINDEX, " +
	                "C.CONGLOMERATENUMBER FROM SYS.SYSCONGLOMERATES C, SYS.SYSTABLES T, SYS.SYSSCHEMAS S " +
	                "WHERE C.TABLEID = T.TABLEID AND T.SCHEMAID = S.SCHEMAID AND T.TABLENAME NOT LIKE 'SYS%' " +
	                "ORDER BY S.SCHEMANAME").executeQuery();

	        ExecRow template;
	        try {
	            DataValueDescriptor[] columns = new DataValueDescriptor[SpliceAdmin.SCHEMA_INFO_COLUMNS.length];
	            for(int i=0;i<SpliceAdmin.SCHEMA_INFO_COLUMNS.length;i++){
	                columns[i] = SpliceAdmin.SCHEMA_INFO_COLUMNS[i].getType().getNull();
	            }
	            template = new ValueRow(columns.length);
	            template.setRowArray(columns);
	        } catch (StandardException e) {
	            throw PublicAPI.wrapStandardException(e);
	        }
	        List<ExecRow> results = Lists.newArrayList();

	        // Map<regionNameAsString,HServerLoad.RegionLoad>
	        Map<String, RegionLoad> regionLoadMap = getRegionLoad();
	        HBaseAdmin admin = null;
	        try {
	            admin = SpliceUtils.getAdmin();
	            StringBuilder regionBuilder = new StringBuilder();
	            while (allTablesInSchema.next()) {
	                String conglom = allTablesInSchema.getObject("CONGLOMERATENUMBER").toString();
	                regionBuilder.setLength(0);
	                for (HRegionInfo ri : admin.getTableRegions(Bytes.toBytes(conglom))) {
	                    String regionName = Bytes.toString(ri.getRegionName());
	                    int storefileSizeMB = 0;
	                    int memStoreSizeMB = 0;
	                    int storefileIndexSizeMB = 0;
	                    if (regionName != null && ! regionName.isEmpty()) {
	                        RegionLoad regionLoad = regionLoadMap.get(regionName);
	                        if (regionLoad != null) {
	                            storefileSizeMB = regionLoad.getStorefileSizeMB();
	                            memStoreSizeMB = regionLoad.getMemStoreSizeMB();
	                            storefileIndexSizeMB = regionLoad.getStorefileIndexSizeMB();

	                            byte[][] parsedRegionName = HRegionInfo.parseRegionName(ri.getRegionName());
	                            String tableName = "Unknown";
	                            String regionID = "Unknown";
	                            if (parsedRegionName != null) {
	                                if (parsedRegionName.length >= 1) {
	                                    tableName = Bytes.toString(parsedRegionName[0]);
	                                }
	                                if (parsedRegionName.length >= 3) {
	                                    regionID = Bytes.toString(parsedRegionName[2]);
	                                }
	                            }
	                            regionBuilder.append('(')
	                                    .append(tableName).append(',')
	                                    .append(regionID).append(' ')
	                                    .append(storefileSizeMB).append(' ')
	                                    .append(memStoreSizeMB).append(' ')
	                                    .append(storefileIndexSizeMB)
	                                    .append(" MB) ");
	                        }
	                    }
	                    DataValueDescriptor[] cols = template.getRowArray();
	                    try {
	                        cols[0].setValue(allTablesInSchema.getString("SCHEMANAME"));
	                        cols[1].setValue(allTablesInSchema.getString("TABLENAME"));
	                        cols[2].setValue(regionName);
	                        cols[3].setValue(allTablesInSchema.getBoolean("ISINDEX"));
	                        cols[4].setValue(storefileSizeMB);
	                        cols[5].setValue(memStoreSizeMB);
	                        cols[6].setValue(storefileIndexSizeMB);
	                    } catch (StandardException e) {
	                        throw PublicAPI.wrapStandardException(e);
	                    }
	                    results.add(template.getClone());
	                }
	            }
	        } catch (IOException e) {
	            throw new SQLException(e);
	        } finally {
	            if (admin != null) {
	                try {
	                    admin.close();
	                } catch (IOException e) {
	                    // ignore
	                }
	            }
	            if (allTablesInSchema != null) {
	                try {
	                    allTablesInSchema.close();
	                } catch (SQLException e) {
	                    // ignore
	                }
	            }
	        }
	        EmbedConnection defaultConn = (EmbedConnection) SpliceAdmin.getDefaultConn();
	        Activation lastActivation = defaultConn.getLanguageConnection().getLastActivation();
	        IteratorNoPutResultSet resultsToWrap = new IteratorNoPutResultSet(results, SpliceAdmin.SCHEMA_INFO_COLUMNS, lastActivation);
	        try{
	            resultsToWrap.openCore();
	            EmbedResultSet ers = new EmbedResultSet40(defaultConn, resultsToWrap, false, null, true);
	            resultSet[0] = ers;
	        } catch (StandardException se) {
	            throw PublicAPI.wrapStandardException(se);
	        }
			
		}

	    public void SYSCS_GET_REGION_SERVER_STATS_INFO(final ResultSet[] resultSet, List<Pair<String, JMXConnector>> connections) throws SQLException {
			ObjectName regionServerStats = null;
            try {
                regionServerStats = JMXUtils.getRegionServerStatistics();
            } catch (MalformedObjectNameException e) {
                throw new SQLException(e);
            }
            ExecRow template = new ValueRow(9);
            template.setRowArray(new DataValueDescriptor[]{
                    new SQLVarchar(),new SQLInteger(),new SQLLongint(),new SQLLongint(),
                    new SQLLongint(),new SQLLongint(),new SQLReal(),new SQLInteger(),new SQLInteger()
            });
            int i = 0;
            List<ExecRow> rows = Lists.newArrayListWithExpectedSize(connections.size());
            for (Pair<String, JMXConnector> mxc : connections) {
        		MBeanServerConnection mbsc;
            	try {
            		mbsc = mxc.getSecond().getMBeanServerConnection();
            	} catch (IOException e) {
            		throw new SQLException(e);
            	}
                template.resetRowArray();
                DataValueDescriptor[] dvds = template.getRowArray();
                try{
                	int idx = 0;
                    dvds[idx++].setValue(connections.get(i).getFirst());
                    dvds[idx++].setValue(((Long)mbsc.getAttribute(regionServerStats,"regionCount")).longValue());
                    dvds[idx++].setValue(((Long)mbsc.getAttribute(regionServerStats,"storeFileCount")).longValue());
                    dvds[idx++].setValue(((Long)mbsc.getAttribute(regionServerStats,"writeRequestCount")).longValue());
                    dvds[idx++].setValue(((Long)mbsc.getAttribute(regionServerStats,"readRequestCount")).longValue());
                    dvds[idx++].setValue(((Long)mbsc.getAttribute(regionServerStats,"totalRequestCount")).floatValue());
                    dvds[idx++].setValue(((Integer)mbsc.getAttribute(regionServerStats,"compactionQueueLength")).intValue());
                    dvds[idx++].setValue(((Integer)mbsc.getAttribute(regionServerStats,"flushQueueLength")).intValue());
                }catch(StandardException se){
                    throw PublicAPI.wrapStandardException(se);
                } catch (Exception e) {
                    throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
                }
                rows.add(template.getClone());
                i++;
            }
            ResultColumnDescriptor[] columnInfo = new ResultColumnDescriptor[8];
            int idx = 0;
            columnInfo[idx++] = new GenericColumnDescriptor("host",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR));
            columnInfo[idx++] = new GenericColumnDescriptor("regionCount",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT));
            columnInfo[idx++] = new GenericColumnDescriptor("storeFileCount",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT));
            columnInfo[idx++] = new GenericColumnDescriptor("writeRequestCount",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT));
            columnInfo[idx++] = new GenericColumnDescriptor("readRequestCount",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT));
            columnInfo[idx++] = new GenericColumnDescriptor("totalRequestCount",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT));
            columnInfo[idx++] = new GenericColumnDescriptor("compactionQueueLength",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER));
            columnInfo[idx++] = new GenericColumnDescriptor("flushQueueLength",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER));

            EmbedConnection defaultConn = (EmbedConnection) BaseAdminProcedures.getDefaultConn();
            Activation lastActivation = defaultConn.getLanguageConnection().getLastActivation();
            IteratorNoPutResultSet resultsToWrap = new IteratorNoPutResultSet(rows, columnInfo,lastActivation);
            try {
                resultsToWrap.openCore();
            } catch (StandardException e) {
                throw PublicAPI.wrapStandardException(e);
            }
            EmbedResultSet ers = new EmbedResultSet40(defaultConn, resultsToWrap,false,null,true);

            resultSet[0] = ers;
	    }

		public ObjectName getRegionServerStatistics() throws MalformedObjectNameException {
			return JMXUtils.getDynamicMBean(REGION_SERVER_STATISTICS);
		}
		
		@Override
		public ServerName getServerName(String serverName) {
			return ServerName.valueOf(serverName);
		}

		@Override
		public ExceptionTranslator getExceptionHandler() {
			return Hbase98ExceptionTranslator.INSTANCE;
		}

		@Override
		public SpliceRegionScanner getSplitRegionScanner(Scan scan, Table htable) throws IOException {
            try {
                return new SplitRegionScanner(scan, htable, HBaseTableFactory.getInstance().getRegionsInRange(htable.getName().getQualifier(), scan.getStartRow(), scan.getStopRow()));
            } catch (Exception e) {
                throw new IOException(e);
            }
		}

		@Override
		public KeyValueScanner getMemstoreFlushAwareScanner(HRegion region,
				Store store, ScanInfo scanInfo, Scan scan,
				NavigableSet<byte[]> columns, long readPt,
				AtomicReference<MemstoreAware> memstoreAware,
				MemstoreAware initialValue) throws IOException {
			return new MemStoreFlushAwareScanner(region,store,scanInfo,scan,columns,readPt,memstoreAware,initialValue);
		}

    @Override
    public SubregionSplitter getSubregionSplitter() {
        return new SubregionSplitter() {
            @Override
            public List<InputSplit> getSubSplits(Table table, List<InputSplit> splits) {
                List<InputSplit> results = new ArrayList<>();
                for (InputSplit split : splits) {
                    final TableSplit tableSplit = (TableSplit) split;
                    try {
                        byte[] probe;
                        byte[] start = tableSplit.getStartRow();
                        if (start.length == 0) {
                            // first region, pick smallest rowkey possible
                            probe = new byte[] { 0 };
                        } else  {
                            // any other region, pick start row
                            probe = start;
                        }

                        Map<byte[], List<InputSplit>> splitResults = table.coprocessorService(SpliceMessage.SpliceDerbyCoprocessorService.class, probe, probe,
                                new Batch.Call<SpliceMessage.SpliceDerbyCoprocessorService, List<InputSplit>>() {
                                    @Override
                                    public List<InputSplit> call(SpliceMessage.SpliceDerbyCoprocessorService instance) throws IOException {
                                        SpliceRpcController controller = new SpliceRpcController();
                                        byte[] startKey = tableSplit.getStartRow();
                                        byte[] stopKey = tableSplit.getEndRow();

                                        SpliceMessage.SpliceSplitServiceRequest message = SpliceMessage.SpliceSplitServiceRequest.newBuilder().setBeginKey(ByteString.copyFrom(startKey)).setEndKey(ByteString.copyFrom(stopKey)).build();

                                        BlockingRpcCallback<SpliceMessage.SpliceSplitServiceResponse> rpcCallback = new BlockingRpcCallback();
                                        instance.computeSplits(controller, message, rpcCallback);
                                        SpliceMessage.SpliceSplitServiceResponse response = rpcCallback.get();
                                        List<InputSplit> result = new ArrayList<InputSplit>();
                                        byte[] first = startKey;
                                        for (ByteString cutpoint : response.getCutPointList()) {
                                            byte[] end = cutpoint.toByteArray();
                                            result.add(new SMSplit(new TableSplit(tableSplit.getTable(), first, end, tableSplit.getRegionLocation())));
                                            first = end;
                                        }
                                        result.add(new SMSplit(new TableSplit(tableSplit.getTable(), first, stopKey, tableSplit.getRegionLocation())));
                                        return result;
                                    }
                                });
                        for (List<InputSplit> value : splitResults.values()) {
                            results.addAll(value);
                        }
                    } catch (Throwable throwable) {
                        throw new RuntimeException(throwable);
                    }
                }
                return results;
            }
        };
    }
}