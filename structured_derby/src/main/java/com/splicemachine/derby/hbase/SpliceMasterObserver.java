package com.splicemachine.derby.hbase;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.error.SpliceDoNotRetryIOException;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.tools.EmbedConnectionMaker;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceUtilities;
import com.splicemachine.utils.ZkUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

public class SpliceMasterObserver extends BaseMasterObserver {
    public static final byte[] INIT_TABLE = Bytes.toBytes("SPLICE_INIT");
    public static final byte[] RESTORE_TABLE = Bytes.toBytes("SPLICE_RESTORE");   
    public static final String BACKUP_PATH = "BACKUP_PATH";       
    private ExecutorService executor;
    protected static final AtomicReference<State> state = new AtomicReference<State>();
    private volatile Future<Void> createFuture;
    private MasterServices masterServices;

    public static enum State {
        NOT_STARTED,
        INITIALIZING,
        RUNNING
    }

    private static Logger LOG = Logger.getLogger(SpliceMasterObserver.class);

    @Override
    public void start(CoprocessorEnvironment ctx) throws IOException {
    	masterServices = ((MasterCoprocessorEnvironment) ctx).getMasterServices();
    	
        SpliceLogUtils.debug(LOG, "Starting Splice Master Observer");
        ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("splice-master-manager").build();
        executor = Executors.newSingleThreadExecutor(factory);
        state.set(State.NOT_STARTED);
        super.start(ctx);
    }

    @Override
    public void stop(CoprocessorEnvironment ctx) throws IOException {
        SpliceLogUtils.debug(LOG, "Stopping Splice Master Observer");
        super.stop(ctx);
    }

    @Override
    public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx, HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
        SpliceLogUtils.info(LOG, "preCreateTable %s", Bytes.toString(desc.getName()));
        if (Bytes.equals(desc.getName(), INIT_TABLE)) {
            try {
                SpliceLogUtils.info(LOG, "Creating Splice");
                evaluateState();
                createSplice();
                throw new PleaseHoldException("pre create succeeeded");
            } catch (PleaseHoldException phe) {
                throw phe;
            } catch (DoNotRetryIOException dnr) {
                throw dnr;
            } catch (Exception e) {
                SpliceLogUtils.logAndThrow(LOG, "preCreateTable Error", Exceptions.getIOException(e));
            }
        } 
        if (Bytes.equals(desc.getName(), RESTORE_TABLE)) {
        	restoreDatabase(desc.getValue(BACKUP_PATH));
        }
        else
    		super.preCreateTable(ctx, desc, regions);
    }

    protected void evaluateState() throws Exception {
        //check for startup errors
        if(createFuture!=null && createFuture.isDone()){
            createFuture.get(); //this will throw an error if the startup sequence fails
        }
        boolean success;
        do{
            State currentState = state.get();
            switch (currentState) {
                case INITIALIZING:
                    throw new PleaseHoldException("Please Hold - Starting");
                case RUNNING:
                    throw new SpliceDoNotRetryIOException("Success");
                case NOT_STARTED:
                    success = state.compareAndSet(currentState, State.INITIALIZING);
                    break;
                default:
                    throw new IllegalStateException("Unable to process Startup state "+ currentState);
            }
        }while(!success);
    }

    private void createSplice() throws Exception {
        createFuture = executor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                Connection connection = null;
                try {
                    if (ZkUtils.isSpliceLoaded()) {
                        SpliceLogUtils.info(LOG, "Splice Already Loaded");
                        state.set(State.RUNNING);
                        return null;
                    } else {
                        SpliceLogUtils.info(LOG, "Booting Splice");
                        ZkUtils.refreshZookeeper();
                        SpliceUtilities.refreshHbase();
                        SpliceUtilities.createSpliceHBaseTables();
                        new SpliceAccessManager(); //make sure splice access manager gets loaded
                        //make sure that we have a Snowflake loaded
                        SpliceDriver.driver().loadUUIDGenerator();
                        EmbedConnectionMaker maker = new EmbedConnectionMaker();
                        connection = maker.createFirstNew();
                        ZkUtils.spliceFinishedLoading();
                        state.set(State.RUNNING);
                        return null;
                    }
                } catch (Exception e) {
                    SpliceLogUtils.logAndThrow(LOG, e);
                } finally {
                    if (connection != null)
                        connection.close();
                }
                return null;

            }

        });
    }

    
    public void restoreDatabase(String backupDirectory) throws IOException {
    	if (LOG.isInfoEnabled())
    		SpliceLogUtils.info(LOG, "restoring database");
    	MasterFileSystem fileSystemManager = masterServices.getMasterFileSystem();
    	    CatalogTracker catalogTracker = masterServices.getCatalogTracker();
    	    FileSystem fs = fileSystemManager.getFileSystem();
    	    Path rootDir = fileSystemManager.getRootDir();
    	    Map<String,HTableDescriptor> descriptors = masterServices.getTableDescriptors().getAll();
    	    for (String tableName: descriptors.keySet()) {
    	    	if (LOG.isInfoEnabled())
    	    		SpliceLogUtils.info(LOG, "Deleting table descriptor " + tableName);
    	    	masterServices.getTableDescriptors().remove(tableName);
    	    }
    	    
    	    
    	    FileStatus[] tableDirectories = FSUtils.listStatus(fs, new Path(backupDirectory+"/BACKUP"));
    	    if (tableDirectories == null) return;

    	    Set<String> table = new HashSet<String>();
    	    for (FileStatus tableDir: tableDirectories) {
    	    	if (LOG.isInfoEnabled())
    	    		SpliceLogUtils.info(LOG, "Restoring table descriptor for %s",tableDir.getPath().getName());
    	    	HTableDescriptor td = loadTableDescriptorFromFile(tableDir.getPath().toString()+"/.tableinfo");
    	    	if (LOG.isInfoEnabled())
    	    		SpliceLogUtils.info(LOG, "Restoring table descriptor for %s {%s}",tableDir.getPath().getName(),td.getNameAsString());
    	    	masterServices.getTableDescriptors().add(td);
    	    	FileStatus[] regionDirectories = FSUtils.listStatus(fs, tableDir.getPath(),new PathFilter() {
					@Override
					public boolean accept(Path path) {
						return !path.getName().startsWith(".");
					}
    	    		
    	    	});
        	    List<HRegionInfo> regions = new ArrayList<HRegionInfo>();
        	    for (FileStatus regionDir: regionDirectories) {
        	    	if (LOG.isInfoEnabled())
        	    		SpliceLogUtils.info(LOG, "Restoring region" + regionDir.getPath().toString());        	    	
        	    	HRegionInfo hri = HRegion.loadDotRegionInfoFileContent(fs, regionDir.getPath());
        	    	if (LOG.isInfoEnabled())
        	    		SpliceLogUtils.info(LOG, "Restoring region with hri " + hri);        	    	
        	    	regions.add(hri);
        	    }
      	      MetaEditor.mutateRegions(catalogTracker, new ArrayList<HRegionInfo>(), regions);
        	    
    	    }
/*    	    	regions.add(regionDir.getPath().getName());
    	    }

    	        	    
    	    for (FileStatus regionDir: regionDirs) {
    	        HRegionInfo hri = HRegion.loadDotRegionInfoFileContent(fs, regionDir.getPath());
    	        regions.add(hri);
    	      }

  */  	    
    	    
    	    
    	    
        	/*
    	    byte[] tableName = hTableDescriptor.getName();
    	    Path tableDir = HTableDescriptor.getTableDir(rootDir, tableName);

    	    try {
    	      // 1. Update descriptor
    	      this.masterServices.getTableDescriptors().add(hTableDescriptor);

    	      // 2. Execute the on-disk Restore
    	      LOG.debug("Starting restore snapshot=" + SnapshotDescriptionUtils.toString(snapshot));
    	      Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshot, rootDir);
    	      RestoreSnapshotHelper restoreHelper = new RestoreSnapshotHelper(
    	          masterServices.getConfiguration(), fs,
    	          snapshot, snapshotDir, hTableDescriptor, tableDir, monitor, status);
    	      RestoreSnapshotHelper.RestoreMetaChanges metaChanges = restoreHelper.restoreHdfsRegions();

    	      // 3. Applies changes to .META.
    	      hris.clear();
    	      status.setStatus("Preparing to restore each region");
    	      if (metaChanges.hasRegionsToAdd()) hris.addAll(metaChanges.getRegionsToAdd());
    	      if (metaChanges.hasRegionsToRestore()) hris.addAll(metaChanges.getRegionsToRestore());
    	      List<HRegionInfo> hrisToRemove = metaChanges.getRegionsToRemove();
    	      MetaEditor.mutateRegions(catalogTracker, hrisToRemove, hris);

    	      // At this point the restore is complete. Next step is enabling the table.
    	      LOG.info("Restore snapshot=" + SnapshotDescriptionUtils.toString(snapshot) + " on table=" +
    	        Bytes.toString(tableName) + " completed!");
    	    } catch (IOException e) {
    	      String msg = "restore snapshot=" + SnapshotDescriptionUtils.toString(snapshot)
    	          + " failed. Try re-running the restore command.";
    	      LOG.error(msg, e);
    	      monitor.receive(new ForeignException(masterServices.getServerName().toString(), e));
    	      throw new RestoreSnapshotException(msg, e);
    	    } finally {
    	      this.stopped = true;
    	    }
    	*/
    	
    }
    
    public HTableDescriptor loadTableDescriptorFromFile(String fileToLoad) throws IOException{
    	if (LOG.isTraceEnabled())
    		SpliceLogUtils.trace(LOG, "loadTableDescriptorFromFile %s",fileToLoad);
    	FileSystem fs = FileSystem.get(URI.create(fileToLoad),SpliceConstants.config);
    	FSDataInputStream in = fs.open(new Path(fileToLoad));
    	HTableDescriptor td = new HTableDescriptor();
    	td.readFields(in);
    	return td;
    }
    
}