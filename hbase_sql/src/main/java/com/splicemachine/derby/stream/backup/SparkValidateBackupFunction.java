package com.splicemachine.derby.stream.backup;

import com.google.common.collect.Lists;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.function.SpliceFlatMapFunction;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.URI;
import java.util.Iterator;
import java.util.List;

/**
 * Created by jyuan on 3/14/16.
 */
public class SparkValidateBackupFunction extends SpliceFlatMapFunction<SpliceOperation,Iterator<String>,String> implements Externalizable {

    private static final Logger LOG = Logger.getLogger(SparkValidateBackupFunction.class);
    private String tableDir;
    private Configuration conf;
    private boolean initialized = false;

    public SparkValidateBackupFunction() {}

    public SparkValidateBackupFunction(String tableDir) {
        this.tableDir = tableDir;
    }

    @Override
    public Iterable<String> call(Iterator<String> files) throws Exception {
        if (!initialized) {
            init();
        }
        List<String> destFileList = Lists.newArrayList();
        while (files.hasNext()) {
            String fileName = files.next();
            copyHFile(fileName);
            validateHFile(fileName);
        }

        return destFileList;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(tableDir);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        tableDir = in.readUTF();
    }

    private String copyHFile(String HFileName)  throws Exception{
        Path HFilePath = new Path(HFileName);
        Path destFilePath = new Path(tableDir + "/V/" + HFilePath.getName());
        DistCp distCp = new DistCp(conf);
        String args[] = new String[2];
        args[0] = HFilePath.toString();
        args[1] = destFilePath.toString();
        ToolRunner.run(distCp, args);
        return destFilePath.toString();
    }

    private void validateHFile(String HFileName) throws IOException {

        FSDataInputStream srcStream = null;
        FSDataInputStream destStream = null;
        try {
            Path HFilePath = new Path(HFileName);
            Path srcFilePath = new Path(HFilePath.getParent().toString() + "/." + HFilePath.getName() + ".crc");
            Path destFilePath = new Path(tableDir + "/V/." + HFilePath.getName() + ".crc");
            byte[] srcBytes = new byte[32];
            byte[] destBytes = new byte[32];
            FileSystem srcFs = FileSystem.get(URI.create(srcFilePath.toString()), conf);
            FileSystem destFs = FileSystem.get(URI.create(destFilePath.toString()), conf);
            srcStream = srcFs.open(srcFilePath);
            destStream = destFs.open(destFilePath);
            int i = 0;
            int j = 0;
            while ((i = srcStream.read(srcBytes)) > 0 && (j = destStream.read(destBytes)) > 0) {
                if (i != j || Bytes.compareTo(srcBytes, destBytes) != 0) {
                    SpliceLogUtils.warn(LOG, "HFile %s corrupted", HFileName);
                }
            }
            if (i!= 0 || j != 0) {
                SpliceLogUtils.warn(LOG, "HFile %s corrupted", HFileName);
            }
        } catch (IOException e) {
            throw e;
        } finally {
            if (srcStream != null) {
                srcStream.close();
            }

            if (destStream != null) {
                destStream.close();
            }
        }
    }

    private void init() {
        conf = HConfiguration.unwrapDelegate();
        initialized = true;
    }
}
