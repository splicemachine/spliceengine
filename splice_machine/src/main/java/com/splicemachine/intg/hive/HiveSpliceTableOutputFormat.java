package com.splicemachine.intg.hive;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLBlob;
import org.apache.derby.iapi.types.SQLBoolean;
import org.apache.derby.iapi.types.SQLDouble;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.derby.iapi.types.SQLLongint;
import org.apache.derby.iapi.types.SQLSmallint;
import org.apache.derby.iapi.types.SQLVarchar;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableOutputCommitter;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.load.ImportContext;
import com.splicemachine.derby.impl.load.ImportUtils;
import com.splicemachine.derby.utils.marshall.BareKeyHash;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.EntryDataHash;
import com.splicemachine.derby.utils.marshall.HashPrefix;
import com.splicemachine.derby.utils.marshall.KeyEncoder;
import com.splicemachine.derby.utils.marshall.KeyPostfix;
import com.splicemachine.derby.utils.marshall.NoOpDataHash;
import com.splicemachine.derby.utils.marshall.NoOpPostfix;
import com.splicemachine.derby.utils.marshall.NoOpPrefix;
import com.splicemachine.derby.utils.marshall.SaltedPrefix;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.mapreduce.HBaseBulkLoadMapper;
import com.splicemachine.mapreduce.HBaseBulkLoadReducer;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.ActiveWriteTxn;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.kryo.KryoPool;
import com.splicemachine.uuid.Snowflake;
import com.splicemachine.mrio.api.SQLUtil;
import com.splicemachine.mrio.api.SpliceMRConstants;
import com.splicemachine.mrio.api.SpliceOutputFormat;
import com.splicemachine.pipeline.api.RecordingCallBuffer;
import com.splicemachine.pipeline.impl.WriteCoordinator;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.log4j.Logger;

public class HiveSpliceTableOutputFormat extends OutputFormat implements
        HiveOutputFormat<ImmutableBytesWritable, Put>,
        org.apache.hadoop.mapred.OutputFormat<ImmutableBytesWritable, Put>{
    private SpliceOutputFormat outputFormat = new SpliceOutputFormat();
    private static SQLUtil sqlUtil = null;
    private static Configuration conf = null;
    private String spliceTableName;
    protected static String tableID;
    private HashMap<List, List> tableStructure;
    private HashMap<List, List> pks;
    private static Logger Log = Logger.getLogger(HiveSpliceTableOutputFormat.class.getName());

    public Configuration getConf() {
        return this.conf;
    }

    public void setConf(Configuration conf) {
        outputFormat.setConf(conf);
        this.conf = conf;
    }

    @Override
    public void checkOutputSpecs(JobContext arg0) throws IOException,
            InterruptedException {
        // TODO Auto-generated method stub

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext arg0)
            throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        return new TableOutputCommitter();
    }

    public RecordWriter getRecordWriter()
            throws IOException, InterruptedException {
        return new SpliceRecordWriter();
    }

    protected class SpliceRecordWriter implements RecordWriter {

        private com.splicemachine.mrio.api.SpliceOutputFormat.SpliceRecordWriter recordWriter;

        public SpliceRecordWriter() throws IOException, InterruptedException{
            recordWriter = (com.splicemachine.mrio.api.SpliceOutputFormat.SpliceRecordWriter) outputFormat.getRecordWriter(null);
        }


        private KeyEncoder getKeyEncoder(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
            return recordWriter.getKeyEncoder(null);
        }

        private int[] getEncodingColumns(int n) {
            return recordWriter.getEncodingColumns(n);

        }

        private DataHash getRowHash(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
            return recordWriter.getRowHash(spliceRuntimeContext);
        }

        private DataValueDescriptor[] createDVD() throws StandardException
        {
            return recordWriter.createDVD();
        }

        @Override
        public void write(Writable valueWritable)
                throws IOException {
            // TODO Auto-generated method stub
            ExecRow value = ((ExecRowWritable)valueWritable).get();
            recordWriter.write(null, value);
        }


        @Override
        public void close(boolean abort) throws IOException {
            recordWriter.close(null);
        }
    }

    @Override
    public void checkOutputSpecs(FileSystem arg0, JobConf jc)
            throws IOException {

        spliceTableName = jc.get(SpliceSerDe.SPLICE_OUTPUT_TABLE_NAME);
        Log.info("checking outputspec, writing to Splice table: "+spliceTableName);
        jc.set(TableOutputFormat.OUTPUT_TABLE, spliceTableName);
        Job job = new Job(jc);
        JobContext jobContext = ShimLoader.getHadoopShims().newJobContext(job);

        try {
            checkOutputSpecs(jobContext);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }

    }

    @Override
    public org.apache.hadoop.mapred.RecordWriter<ImmutableBytesWritable, Put> getRecordWriter(
            FileSystem arg0, JobConf arg1, String arg2, Progressable arg3)
            throws IOException {
        // TODO Auto-generated method stub
        throw new RuntimeException("Error: Hive should not invoke this method.");
    }

    @Override
    public org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter getHiveRecordWriter(
            JobConf jc, Path finalOutPath,
            Class<? extends Writable> valueClass, boolean isCompressed,
            Properties tableProperties, Progressable progress)
            throws IOException {
        String spliceTableName = jc.get(SpliceSerDe.SPLICE_OUTPUT_TABLE_NAME);
        jc.set(TableOutputFormat.OUTPUT_TABLE, spliceTableName);
        final boolean walEnabled = HiveConf.getBoolVar(
                jc, HiveConf.ConfVars.HIVE_HBASE_WAL_ENABLED);

        setConf(jc);
        RecordWriter rw = null;
        try {
            rw = this.getRecordWriter();
        } catch (InterruptedException e) {
            Log.error(e);
            System.exit(1);
        }
        return rw;
    }

    @Override
    public org.apache.hadoop.mapreduce.RecordWriter getRecordWriter(
            TaskAttemptContext arg0) throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        return null;
    }


}


