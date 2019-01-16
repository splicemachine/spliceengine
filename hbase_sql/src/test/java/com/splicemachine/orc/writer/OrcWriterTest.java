/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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
package com.splicemachine.orc.writer;

import com.google.common.collect.ImmutableMap;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.orc.FileOrcDataSource;
import com.splicemachine.orc.OrcReader;
import com.splicemachine.orc.OrcRecordReader;
import com.splicemachine.orc.OrcWriter;
import com.splicemachine.orc.metadata.CompressionKind;
import com.splicemachine.utils.IntArrays;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.SliceOutput;
import io.airlift.units.DataSize;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.ORCFileStorageFormatDescriptor;
import org.apache.hadoop.hive.ql.io.orc.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.hive.HiveInspectors;
import org.apache.spark.sql.hive.orc.OrcFileFormat;
import org.apache.spark.sql.hive.orc.OrcOutputWriter;
import org.apache.spark.sql.hive.orc.OrcSerializer;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.unsafe.types.UTF8String;
import org.joda.time.DateTimeZone;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

/**
 * Created by jleach on 8/21/17.
 */
@Ignore
public class OrcWriterTest {


    @Test
    public void writeOrcFile() throws Exception {
        ExecRow foo = new ValueRow(3);
        foo.setRowArray(new DataValueDescriptor[]{new SQLVarchar(),new SQLInteger(),new SQLLongint()});
        StructType type = foo.createStructType(IntArrays.count(foo.size()));
        TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(type.catalogString());
        SettableStructObjectInspector oi = (SettableStructObjectInspector) OrcStruct.createObjectInspector(typeInfo);
        OrcSerializer orcSerializer = new OrcSerializer(type,new Configuration());
        OrcStruct orcStruct = (OrcStruct) oi.create();
        Writer writer = OrcFile.createWriter(FileSystem.get(new URI("/tmp/ugh"),new Configuration()),new Path("/tmp/ugh"), new Configuration(),oi,1000000, org.apache.hadoop.hive.ql.io.orc.CompressionKind.ZLIB,100000,10000);
        InternalRow row = new GenericInternalRow(new Object[]{UTF8String.fromString("12312321"), new Integer(1), new Long(1)});
        List structFields = oi.getAllStructFieldRefs();
        for (int j = 0; j< structFields.size(); j++) {
            oi.setStructFieldData(orcStruct,(org.apache.hadoop.hive.serde2.objectinspector.StructField) structFields.get(j),
                    orcSerializer.wrap(
                    row.get(j,type.fields()[j].dataType()),
                            ((org.apache.hadoop.hive.serde2.objectinspector.StructField) structFields.get(j)).getFieldObjectInspector(),
                            type.fields()[j].dataType()));
        }


        for (int i = 0; i< 1000; i++) {
            for (int j = 0; j< structFields.size(); j++) {
                oi.setStructFieldData(orcStruct,(org.apache.hadoop.hive.serde2.objectinspector.StructField) structFields.get(j),
                        orcSerializer.wrap(
                                row.get(j,type.fields()[j].dataType()),
                                ((org.apache.hadoop.hive.serde2.objectinspector.StructField) structFields.get(j)).getFieldObjectInspector(),
                                type.fields()[j].dataType()));
            }
            writer.addRow(orcStruct);
        }
        System.out.println("Foo" + writer.getNumberOfRows());
        System.out.println("Foo" + writer.getRawDataSize());

        writer.close();

        Reader reader = OrcFile.createReader(FileSystem.get(new URI("/tmp/ugh"),new Configuration()),new Path("/tmp/ugh"));
        System.out.println(reader.getNumberOfRows());
        System.out.println(Arrays.toString(reader.getStatistics()));
        RecordReader rr = reader.rows();
        while (rr.hasNext()) {
            System.out.println(rr.next(null));
        }

    }

    @Test
    public void testSimpleOrcWriter() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream daos = new DataOutputStream(baos);
        SliceOutput sliceOutput = new OutputStreamSliceOutput(daos);


        List<String> names = new ArrayList<>(6);
        names.add("1");
        names.add("2");
        names.add("3");
        names.add("4");
        List<DataType> dataTypes = new ArrayList<>(6);
        dataTypes.add(VarcharType.apply(256));
        dataTypes.add(CharType.apply(256));
        dataTypes.add(DataTypes.IntegerType);
        dataTypes.add(DataTypes.LongType);
        OrcWriter orcWriter = OrcWriter.createOrcWriter(sliceOutput,names,dataTypes,CompressionKind.ZLIB,
                new DataSize(124, MEGABYTE),1,10000000,10000000,new DataSize(124, MEGABYTE),
                ImmutableMap.<String, String>builder()
                        .put("SpliceMachine", "3.0")
                        .build(),DateTimeZone.UTC,false);
        ExecRow execRow = new ValueRow(4);
        execRow.setRowArray(new DataValueDescriptor[]{
                new SQLVarchar(),
                new SQLChar(),
                new SQLInteger(),
                new SQLLongint(),
        });
        SpliceBlock block = new SpliceBlock(execRow);
        for (int i = 0; i < 100; i++) {
            execRow.getColumn(1).setValue("foo"+i);
            execRow.getColumn(2).setValue("foo"+i);
            execRow.getColumn(4).setValue(i);
            block.addExecRow(execRow);
        }
        orcWriter.write(block);
        sliceOutput.flush();
        System.out.println(baos.size());

    }
}