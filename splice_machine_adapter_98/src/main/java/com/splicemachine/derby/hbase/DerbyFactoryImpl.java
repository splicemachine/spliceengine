package com.splicemachine.derby.hbase;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.PublicAPI;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.impl.jdbc.EmbedResultSet;
import org.apache.derby.impl.jdbc.EmbedResultSet40;
import org.apache.derby.impl.sql.execute.IteratorNoPutResultSet;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.operation.SuccessFilter;
import com.splicemachine.derby.impl.job.scheduler.BaseJobControl;
import com.splicemachine.derby.impl.job.scheduler.JobControl;
import com.splicemachine.derby.impl.job.scheduler.JobMetrics;
import com.splicemachine.derby.impl.sql.execute.DropIndexConstantOperation;
import com.splicemachine.derby.impl.sql.execute.operations.SkippingScanFilter;
import com.splicemachine.derby.impl.store.access.base.SpliceGenericCostController;
import com.splicemachine.derby.utils.SpliceAdmin;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.hbase.HBaseRegionLoads;
import com.splicemachine.hbase.HBaseServerUtils;
import com.splicemachine.hbase.debug.HBaseEntryPredicateFilter;
import com.splicemachine.pipeline.api.BulkWritesInvoker.Factory;
import com.splicemachine.pipeline.impl.BulkWritesRPCInvoker;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceZooKeeperManager;

public class DerbyFactoryImpl implements DerbyFactory {

	@Override
	public Filter getAllocatedFilter(byte[] localAddress) {
		return new AllocatedFilter(localAddress);
	}

	@Override
	public SpliceBaseOperationRegionScanner getOperationRegionScanner(
			RegionScanner s, Scan scan, HRegion region,
			TransactionalRegion txnRegion) throws IOException {
		return new SpliceOperationRegionScanner(s,scan,region,txnRegion);
	}

	@Override
	public List<HRegion> getOnlineRegions(RegionServerServices services,
			byte[] tableName) throws IOException {
		try {
		return services.getOnlineRegions(TableName.valueOf(tableName));
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public void removeTableFromDescriptors(MasterServices masterServices,
			String tableName) {
		try {
	      masterServices.getTableDescriptors().remove(TableName.valueOf(tableName));
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
	public BaseJobControl getJobControl(CoprocessorJob job, String jobPath,
			SpliceZooKeeperManager zkManager, int maxResubmissionAttempts,
			JobMetrics jobMetrics) {
		return new JobControl(job,jobPath,zkManager,maxResubmissionAttempts,jobMetrics);
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
			Path regiondir, FileSystem fs, Configuration conf)
			throws IOException {
		HRegionFileSystem.createRegionOnFileSystem(conf, fs, regiondir, regionInfo);
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
	public boolean isCallTimeoutException(Throwable t) {
		return t instanceof RpcClient.CallTimeoutException;
	}
	@Override
	public boolean isFailedServerException(Throwable t) {
		return t instanceof RpcClient.FailedServerException;
	}

	@Override
	public Factory getBulkWritesInvoker(HConnection connection, byte[] tableName) {
		return new BulkWritesRPCInvoker.Factory(connection,tableName);
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
		public Filter getSuccessFilter(List<byte[]> failedTasks) {
			return new SuccessFilter(failedTasks);
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
		public HTableInterface getTable(RegionCoprocessorEnvironment rce,
				byte[] tableName) throws IOException {
			return rce.getTable(TableName.valueOf(tableName));
		}
		@Override
		public int getReduceNumberOfRegions(String tableName, Configuration conf) throws IOException {
			return MetaReader.getRegionCount(conf, tableName);
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
}