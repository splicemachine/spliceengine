package com.splicemachine.derby.impl.load;

import com.google.common.io.Closeables;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceZooKeeperManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Created on: 8/26/13
 */
public class ImportTask extends ZkTask{
    private static final long serialVersionUID = 2l;
    private static final Logger LOG = Logger.getLogger(ImportTask.class);

    protected ImportContext importContext;
    protected FileSystem fileSystem;
    protected ImportReader reader;

    private Importer importer;

    /**
     * @deprecated only available for a a no-args constructor
     */
    @Deprecated
    @SuppressWarnings("UnusedDeclaration")
    public ImportTask() { }

    public ImportTask(String jobId,
                      ImportContext importContext,
                      ImportReader reader,
                      int priority,
                      String parentTxnId){
        super(jobId,priority,parentTxnId,false);
        this.importContext = importContext;
        this.reader = reader;
    }

    public ImportTask(String jobId,
                      ImportContext importContext,
                      ImportReader reader,
                      Importer importer,
                      int priority,
                      String parentTxnId){
        super(jobId,priority,parentTxnId,false);
        this.importContext = importContext;
        this.reader = reader;
        this.importer = importer;
    }

    @Override
    public void doExecute() throws ExecutionException, InterruptedException {
        try{
            ExecRow row = getExecRow(importContext);
            if(importer==null)
                importer = new ParallelImporter(importContext,
                    row, getTaskStatus().getTransactionId());

            try{
                reader.setup(fileSystem,importContext);
                long numRead=0;
                long totalReadTime=0l;
                try{
                    String[] nextRow;
                    do{
                        long start = System.nanoTime();
                        nextRow = reader.nextRow();
                        long stop = System.nanoTime();
                        totalReadTime+=(stop-start);
                        numRead++;

                        if(nextRow!=null)
                            importer.process(nextRow);
                    }while(nextRow!=null);
                } catch (Exception e) {
                    throw new ExecutionException(e);
                } finally{
                    Closeables.closeQuietly(reader);
                    /*
                     * We don't call closeQuietly(importer) here, because
                     * we need to make sure that we get out any IOExceptions
                     * that get thrown
                     */
                    importer.close();
                    if(LOG.isDebugEnabled()){
                        SpliceLogUtils.debug(LOG,"Total time taken to read %d records: %d ns",numRead,totalReadTime);
                        SpliceLogUtils.debug(LOG,"Average time taken to read 1 record: %f ns",(double)totalReadTime/numRead);
                    }
                }
            }catch(Exception e){
                if(e instanceof ExecutionException)
                    throw (ExecutionException)e;
                throw new ExecutionException(e);
            }
        } catch (StandardException e) {
            throw new ExecutionException(e);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        importContext = (ImportContext)in.readObject();
        reader = (ImportReader)in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(importContext);
        out.writeObject(reader);
    }

    @Override
    public boolean invalidateOnClose() {
        return false;
    }

    @Override
    protected String getTaskType() {
        return "importTask";
    }

    @Override
    public void prepareTask(RegionCoprocessorEnvironment rce,
                            SpliceZooKeeperManager zooKeeper) throws ExecutionException {
        fileSystem = rce.getRegion().getFilesystem();
        super.prepareTask(rce, zooKeeper);
    }

    //exposed to make testing possible without having to mock an entire ZooKeeper setup
    void setFileSystem(FileSystem fileSystem){
        this.fileSystem = fileSystem;
    }

    protected ExecRow getExecRow(ImportContext context) throws StandardException {
        ColumnContext[] cols = context.getColumnInformation();
        int size = cols[cols.length-1].getColumnNumber()+1;
        ExecRow row = new ValueRow(size);
        for(ColumnContext col:cols){
            DataValueDescriptor dataValueDescriptor = getDataValueDescriptor(col);
            row.setColumn(col.getColumnNumber()+1, dataValueDescriptor);
        }
        return row;
    }

    private DataValueDescriptor getDataValueDescriptor(ColumnContext columnContext) throws StandardException {
        DataTypeDescriptor td = DataTypeDescriptor.getBuiltInDataTypeDescriptor(columnContext.getColumnType());
        if(!columnContext.isNullable())
            td = td.getNullabilityType(false);
        return td.getNull();
    }
}
