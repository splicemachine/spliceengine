/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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

