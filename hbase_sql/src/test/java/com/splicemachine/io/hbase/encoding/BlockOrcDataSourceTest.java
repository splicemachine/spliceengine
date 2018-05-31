package com.splicemachine.io.hbase.encoding;

import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.orc.OrcReader;
import com.splicemachine.orc.OrcRecordReader;
import com.splicemachine.orc.input.ColumnarBatchRow;
import com.splicemachine.orc.memory.AggregatedMemoryContext;
import com.splicemachine.orc.metadata.OrcMetadataReader;
import com.splicemachine.orc.predicate.SpliceORCPredicate;
import com.splicemachine.si.data.hbase.coprocessor.SIObserver;
import com.splicemachine.si.impl.SpliceQuery;
import com.splicemachine.utils.IntArrays;
import io.airlift.units.DataSize;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.spark.sql.execution.vectorized.ColumnarBatch;
import org.apache.spark.sql.hive.orc.OrcSerializer;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.types.DataType;
import org.junit.Test;
import org.testng.Assert;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jleach on 11/2/17.
 */
public class BlockOrcDataSourceTest {

    @Test
    public void testWriteReadFromDataSource() throws Exception {
        ExecRow execRow = new ValueRow(new DataValueDescriptor[] {
                new SQLVarchar("foo"),
                new SQLInteger(123)
        });
        ExecRow writableRow = PAXEncodingState.createWritableRow(execRow);
        writableRow.getColumn(1).setValue((byte)0);
        writableRow.getColumn(2).setValue(false);
        writableRow.getColumn(3).setValue(10000);
        writableRow.getColumn(4).setValue(0);
        StructType type = writableRow.createStructType();
        TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(type.catalogString());
        SettableStructObjectInspector oi = (SettableStructObjectInspector) OrcStruct.createObjectInspector(typeInfo);
        OrcSerializer orcSerializer = new OrcSerializer(type,new Configuration());
        OrcStruct orcStruct = (OrcStruct) oi.create();
        ByteArrayOutputStream baosInMemory = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baosInMemory);
        Writer writer = OrcFile.createWriter(new PAXBlockFileSystem(out),new Path("/"), new Configuration(),oi,1000000, org.apache.hadoop.hive.ql.io.orc.CompressionKind.ZLIB,100000,10000);
        List<? extends StructField> structFields = oi.getAllStructFieldRefs();

        for (int i = 0; i < structFields.size(); i++) {
            oi.setStructFieldData(orcStruct, structFields.get(i),
                    orcSerializer.wrap(
                            writableRow.getColumn(i + 1).getSparkObject(),
                            structFields.get(i).getFieldObjectInspector(),
                            type.fields()[i].dataType()));
        }
        writer.addRow(orcStruct);
        writer.close();

        BlockOrcDataSource orcDataSource = new BlockOrcDataSource(ByteBuffer.wrap(baosInMemory.toByteArray()), 0);
        OrcReader reader = new OrcReader(orcDataSource, new OrcMetadataReader(),
                new DataSize(200, DataSize.Unit.MEGABYTE),
                new DataSize(200, DataSize.Unit.MEGABYTE),
                new DataSize(200, DataSize.Unit.MEGABYTE)
        );
        Map<Integer,DataType> typeMap = new HashMap<>(writableRow.nColumns());
        for (int i = 0; i< writableRow.nColumns(); i++) {
            typeMap.put(i,writableRow.getColumn(i+1).getStructField("c"+i).dataType());
        }
        OrcRecordReader recordReader = reader.createRecordReader(typeMap, SpliceORCPredicate.TRUE,PAXEncodedSeeker.HIVE_STORAGE_TIME_ZONE,new AggregatedMemoryContext(), Collections.emptyList(),Collections.EMPTY_LIST);
        recordReader.nextBatch();
        ColumnarBatch columnarBatch = recordReader.getColumnarBatch(writableRow.schema());
        ExecRow blankRow = writableRow.getNewNullRow().fromSparkRow(new ColumnarBatchRow(columnarBatch.getRow(0),writableRow.schema()));
        Assert.assertEquals(1,columnarBatch.numRows());
        Assert.assertEquals(writableRow,blankRow);
    }
}