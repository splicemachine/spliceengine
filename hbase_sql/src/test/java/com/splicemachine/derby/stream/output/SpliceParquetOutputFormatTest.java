/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.stream.output;
/*
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.stream.BaseStreamTest;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hive.ql.io.ParquetFileStorageFormatDescriptor;
import org.apache.spark.sql.columnar.BINARY;
import org.junit.Ignore;
import org.junit.Test;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;
import parquet.schema.OriginalType;
import parquet.schema.Types;

import java.io.IOException;
*/
/**
 * Created by jleach on 5/15/15.
 */
//@Ignore
public class SpliceParquetOutputFormatTest {} /*extends BaseStreamTest {
    protected String baseParquetDirectory = SpliceUnitTest.getBaseDirectory()+"/target/parquet";

    @Test
    public void testWritingParquetFile() throws Exception {
        ParquetWriter<ExecRow> writer = new ParquetWriter<ExecRow>(
                new Path(baseParquetDirectory+"/foo"),
                new SpliceWriteSupport(ParquetExecRowUtils.buildSchemaFromExecRowDefinition(tenRowsTwoDuplicateRecords.get(0))),
                CompressionCodecName.UNCOMPRESSED,ParquetWriter.DEFAULT_BLOCK_SIZE,ParquetWriter.DEFAULT_PAGE_SIZE);
        for (int i =0 ; i< tenRowsTwoDuplicateRecords.size();i++) {
            writer.write(tenRowsTwoDuplicateRecords.get(i));
        }
        writer.close();

        ParquetMetadata readFooter = ParquetFileReader.readFooter(HBaseConfiguration.create(), new Path(baseParquetDirectory + "/foo"));

    }

}
*/

