package org.apache.hadoop.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * @author Scott Fines
 *         Date: 6/8/15
 */
public class ForwardingHConnection implements HConnection{
    private final HConnection connection;

    public ForwardingHConnection(HConnection connection){
        this.connection=connection;
    }

    @Override
    public Configuration getConfiguration(){
        return connection.getConfiguration();
    }

    @Override
    public HTableInterface getTable(String tableName) throws IOException{
        return connection.getTable(tableName);
    }

    @Override
    public HTableInterface getTable(byte[] tableName) throws IOException{
        return connection.getTable(tableName);
    }

    @Override
    public HTableInterface getTable(TableName tableName) throws IOException{
        return connection.getTable(tableName);
    }

    @Override
    public HTableInterface getTable(String tableName,ExecutorService pool) throws IOException{
        return connection.getTable(tableName,pool);
    }

    @Override
    public HTableInterface getTable(byte[] tableName,ExecutorService pool) throws IOException{
        return connection.getTable(tableName,pool);
    }

    @Override
    public HTableInterface getTable(TableName tableName,ExecutorService pool) throws IOException{
        return connection.getTable(tableName,pool);
    }

    @Override
    public RegionLocator getRegionLocator(TableName tableName) throws IOException{
        return connection.getRegionLocator(tableName);
    }

    @Override
    public Admin getAdmin() throws IOException{
        return connection.getAdmin();
    }

    @Override
    @Deprecated
    public boolean isMasterRunning() throws MasterNotRunningException, ZooKeeperConnectionException{
        return connection.isMasterRunning();
    }

    @Override
    public boolean isTableEnabled(TableName tableName) throws IOException{
        return connection.isTableEnabled(tableName);
    }

    @Override
    @Deprecated
    public boolean isTableEnabled(byte[] tableName) throws IOException{
        return connection.isTableEnabled(tableName);
    }

    @Override
    public boolean isTableDisabled(TableName tableName) throws IOException{
        return connection.isTableDisabled(tableName);
    }

    @Override
    @Deprecated
    public boolean isTableDisabled(byte[] tableName) throws IOException{
        return connection.isTableDisabled(tableName);
    }

    @Override
    public boolean isTableAvailable(TableName tableName) throws IOException{
        return connection.isTableAvailable(tableName);
    }

    @Override
    @Deprecated
    public boolean isTableAvailable(byte[] tableName) throws IOException{
        return connection.isTableAvailable(tableName);
    }

    @Override
    @Deprecated
    public boolean isTableAvailable(TableName tableName,byte[][] splitKeys) throws IOException{
        return connection.isTableAvailable(tableName,splitKeys);
    }

    @Override
    @Deprecated
    public boolean isTableAvailable(byte[] tableName,byte[][] splitKeys) throws IOException{
        return connection.isTableAvailable(tableName,splitKeys);
    }

    @Override
    @Deprecated
    public HTableDescriptor[] listTables() throws IOException{
        return connection.listTables();
    }

    @Override
    @Deprecated
    public String[] getTableNames() throws IOException{
        return connection.getTableNames();
    }

    @Override
    @Deprecated
    public TableName[] listTableNames() throws IOException{
        return connection.listTableNames();
    }

    @Override
    @Deprecated
    public HTableDescriptor getHTableDescriptor(TableName tableName) throws IOException{
        return connection.getHTableDescriptor(tableName);
    }

    @Override
    @Deprecated
    public HTableDescriptor getHTableDescriptor(byte[] tableName) throws IOException{
        return connection.getHTableDescriptor(tableName);
    }

    @Override
    @Deprecated
    public HRegionLocation locateRegion(TableName tableName,byte[] row) throws IOException{
        return connection.locateRegion(tableName,row);
    }

    @Override
    @Deprecated
    public HRegionLocation locateRegion(byte[] tableName,byte[] row) throws IOException{
        return connection.locateRegion(tableName,row);
    }

    @Override
    @Deprecated
    public void clearRegionCache(){
        connection.clearRegionCache();
    }

    @Override
    @Deprecated
    public void clearRegionCache(TableName tableName){
        connection.clearRegionCache(tableName);
    }

    @Override
    @Deprecated
    public void clearRegionCache(byte[] tableName){
        connection.clearRegionCache(tableName);
    }

    @Override
    @Deprecated
    public void deleteCachedRegionLocation(HRegionLocation location){
        connection.deleteCachedRegionLocation(location);
    }

    @Override
    @Deprecated
    public HRegionLocation relocateRegion(TableName tableName,byte[] row) throws IOException{
        return connection.relocateRegion(tableName,row);
    }

    @Override
    @Deprecated
    public HRegionLocation relocateRegion(byte[] tableName,byte[] row) throws IOException{
        return connection.relocateRegion(tableName,row);
    }

    @Override
    @Deprecated
    public void updateCachedLocations(TableName tableName,byte[] rowkey,Object exception,HRegionLocation source){
        connection.updateCachedLocations(tableName,rowkey,exception,source);
    }

    @Override
    @Deprecated
    public void updateCachedLocations(TableName tableName,byte[] regionName,byte[] rowkey,Object exception,ServerName source){
        connection.updateCachedLocations(tableName,regionName,rowkey,exception,source);
    }

    @Override
    @Deprecated
    public void updateCachedLocations(byte[] tableName,byte[] rowkey,Object exception,HRegionLocation source){
        connection.updateCachedLocations(tableName,rowkey,exception,source);
    }

    @Override
    @Deprecated
    public HRegionLocation locateRegion(byte[] regionName) throws IOException{
        return connection.locateRegion(regionName);
    }

    @Override
    @Deprecated
    public List<HRegionLocation> locateRegions(TableName tableName) throws IOException{
        return connection.locateRegions(tableName);
    }

    @Override
    @Deprecated
    public List<HRegionLocation> locateRegions(byte[] tableName) throws IOException{
        return connection.locateRegions(tableName);
    }

    @Override
    @Deprecated
    public List<HRegionLocation> locateRegions(TableName tableName,boolean useCache,boolean offlined) throws IOException{
        return connection.locateRegions(tableName,useCache,offlined);
    }

    @Override
    @Deprecated
    public List<HRegionLocation> locateRegions(byte[] tableName,boolean useCache,boolean offlined) throws IOException{
        return connection.locateRegions(tableName,useCache,offlined);
    }

    @Override
    @Deprecated
    public MasterProtos.MasterService.BlockingInterface getMaster() throws IOException{
        return connection.getMaster();
    }

    @Override
    @Deprecated
    public AdminProtos.AdminService.BlockingInterface getAdmin(ServerName serverName) throws IOException{
        return connection.getAdmin(serverName);
    }

    @Override
    @Deprecated
    public ClientProtos.ClientService.BlockingInterface getClient(ServerName serverName) throws IOException{
        return connection.getClient(serverName);
    }

    @Override
    @Deprecated
    public AdminProtos.AdminService.BlockingInterface getAdmin(ServerName serverName,boolean getMaster) throws IOException{
        return connection.getAdmin(serverName,getMaster);
    }

    @Override
    @Deprecated
    public HRegionLocation getRegionLocation(TableName tableName,byte[] row,boolean reload) throws IOException{
        return connection.getRegionLocation(tableName,row,reload);
    }

    @Override
    @Deprecated
    public HRegionLocation getRegionLocation(byte[] tableName,byte[] row,boolean reload) throws IOException{
        return connection.getRegionLocation(tableName,row,reload);
    }

    @Override
    @Deprecated
    public void processBatch(List<? extends Row> actions,TableName tableName,ExecutorService pool,Object[] results) throws IOException, InterruptedException{
        connection.processBatch(actions,tableName,pool,results);
    }

    @Override
    @Deprecated
    public void processBatch(List<? extends Row> actions,byte[] tableName,ExecutorService pool,Object[] results) throws IOException, InterruptedException{
        connection.processBatch(actions,tableName,pool,results);
    }

    @Override
    @Deprecated
    public <R> void processBatchCallback(List<? extends Row> list,TableName tableName,ExecutorService pool,Object[] results,Batch.Callback<R> callback) throws IOException, InterruptedException{
        connection.processBatchCallback(list,tableName,pool,results,callback);
    }

    @Override
    @Deprecated
    public <R> void processBatchCallback(List<? extends Row> list,byte[] tableName,ExecutorService pool,Object[] results,Batch.Callback<R> callback) throws IOException, InterruptedException{
        connection.processBatchCallback(list,tableName,pool,results,callback);
    }

    @Override
    @Deprecated
    public void setRegionCachePrefetch(TableName tableName,boolean enable){
        connection.setRegionCachePrefetch(tableName,enable);
    }

    @Override
    @Deprecated
    public void setRegionCachePrefetch(byte[] tableName,boolean enable){
        connection.setRegionCachePrefetch(tableName,enable);
    }

    @Override
    @Deprecated
    public boolean getRegionCachePrefetch(TableName tableName){
        return connection.getRegionCachePrefetch(tableName);
    }

    @Override
    @Deprecated
    public boolean getRegionCachePrefetch(byte[] tableName){
        return connection.getRegionCachePrefetch(tableName);
    }

    @Override
    @Deprecated
    public int getCurrentNrHRS() throws IOException{
        return connection.getCurrentNrHRS();
    }

    @Override
    @Deprecated
    public HTableDescriptor[] getHTableDescriptorsByTableName(List<TableName> tableNames) throws IOException{
        return connection.getHTableDescriptorsByTableName(tableNames);
    }

    @Override
    @Deprecated
    public HTableDescriptor[] getHTableDescriptors(List<String> tableNames) throws IOException{
        return connection.getHTableDescriptors(tableNames);
    }

    @Override
    public boolean isClosed(){
        return connection.isClosed();
    }

    @Override
    @Deprecated
    public void clearCaches(ServerName sn){
        connection.clearCaches(sn);
    }

    @Override
    @Deprecated
    public MasterKeepAliveConnection getKeepAliveMasterService() throws MasterNotRunningException{
        return connection.getKeepAliveMasterService();
    }

    @Override
    @Deprecated
    public boolean isDeadServer(ServerName serverName){
        return connection.isDeadServer(serverName);
    }

    @Override
    @Deprecated
    public NonceGenerator getNonceGenerator(){
        return connection.getNonceGenerator();
    }

    @Override
    public BufferedMutator getBufferedMutator(TableName tableName) throws IOException{
        return connection.getBufferedMutator(tableName);
    }

    @Override
    public BufferedMutator getBufferedMutator(BufferedMutatorParams params) throws IOException{
        return connection.getBufferedMutator(params);
    }

    @Override
    public void close() throws IOException{
    } //no-op

    @Override
    public void abort(String why,Throwable e){
        connection.abort(why,e);
    }

    @Override
    public boolean isAborted(){
        return connection.isAborted();
    }
}
