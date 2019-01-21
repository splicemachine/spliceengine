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
package com.splicemachine.io.hbase.encoding;

import com.splicemachine.art.node.Base;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.orc.OrcWriter;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.SettableSequenceId;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.spark.sql.hive.orc.OrcSerializer;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Test;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.apache.hadoop.hive.ql.io.orc.CompressionKind.NONE;

/**
 * Created by jleach on 11/16/17.
 */
public class PAXEncodingStateTest {

/*    @Test
    public void test1AndNegative1() {
        byte[] foo = new byte[8];
        byte[] foo2 = new byte[8];
        PAXEncodingState.writeDescendingLong(foo,16,-1L);
        PAXEncodingState.writeDescendingLong(foo2,16,1L);

        System.out.println(Bytes.toHex(foo));
        System.out.println(Bytes.toHex(foo2));
        System.out.println(Bytes.compareTo(foo,foo2));
        System.out.println(Byte.compare(foo[0],foo2[0]));
    }
*/
    @Test
    public void testIncrementSeek() {
        byte[] key = Base.fromHex("C064");
        byte[] newKey = Bytes.add(key, new byte[1]);
        byte[] term = new byte[key.length+1];
        System.arraycopy(key,0,term,0,2);
        term[key.length] = 0x00;

        System.out.println(Bytes.toHex(newKey));
        System.out.println(Bytes.toHex(term));

        System.out.println(Bytes.compareTo(key,newKey));
        System.out.println(Bytes.compareTo(newKey,term));

    }

/*
    @Test
    public void testWriteDescendingLongWithRandomSortedLongs() {
        byte[] prior = new byte[8];
        byte[] current = new byte[8];
        Random random = new Random();
        List<Long> randomLongs = new ArrayList<>(1000);
        for (int i = 0; i< 1000; i++) {
            randomLongs.add(random.nextLong());
        }
        Collections.sort(randomLongs);
        for (int i = 0; i< 1000; i++) {
            if (i != 0L) {
                PAXEncodingState.writeDescendingLong(current,16,randomLongs.get(i).longValue());
                Assert.assertTrue("i->"+i, (Bytes.compareTo(prior,current)> 0));
            }
            PAXEncodingState.writeDescendingLong(prior,16,randomLongs.get(i).longValue());
        }
    }
*/
    @Test
    public void debug() {
        int compare = Bytes.compareTo(
                Bytes.fromHex("0515FD0B3985A0018000000000000000007FFFFFFFFFFFFFFF"),Bytes.fromHex("0515FD0B3985A0017FFFFFFFFFFFFFFE047FFFFFFFFFFFFFE5"));
        System.out.println(compare);


        int compare2 = Bytes.compareTo(
                Bytes.fromHex("0315FD0185CC500100000000000000000E7FFFFFFFFFFFFFFF"),
                Bytes.fromHex("0315FD0185CC50017FFFFFFFFFFFFFFE047FFFFFFFFFFFFFFA"));
        System.out.println(compare2);

        int compare3 = Bytes.compareTo(
                Bytes.fromHex("3A323232323233672F323266322F686639392F3567663A2F3232326332633264333B32320045514E574F5046474843574E564B467FFFFFFFFFFFFFFE047FFFFFFFFFFFFFDE"),
                Bytes.fromHex("3A323232323233672F323266322F686639392F3567663A2F3232326332633264333B32320045514E574F5046474843574E568000000000000000007FFFFFFFFFFFFFFF"));
        System.out.println(compare3);


        int compare4 = Bytes.compareTo(
                Bytes.fromHex("00000000000003008000000000000000007FFFFFFFFFFFFFFF"),
                Bytes.fromHex("00000000000003007FFFFFFFFFFFFFFE047FFFFFFFFFFFFFC9"));
        System.out.println(compare4);




        int compare5 = Bytes.compareTo(
                Bytes.fromHex("7F"),
                Bytes.fromHex("80")
                );

        System.out.println(compare5);

    }





/*
    @Test
    public void testCellToKey() throws Exception {
        Cell foo = new KeyValue("123".getBytes(), SIConstants.DEFAULT_FAMILY_ACTIVE_BYTES,SIConstants.PACKED_COLUMN_BYTES,12, KeyValue.Type.Put,"value".getBytes());
        ((SettableSequenceId)foo).setSequenceId(100);
        byte[] rowKey = PAXEncodingState.genRowKey(foo);
        Assert.assertEquals(12L,PAXEncodingState.readTimeStamp(rowKey));
        Assert.assertEquals(KeyValue.Type.Put,KeyValue.Type.codeToType(PAXEncodingState.readTypeByte(rowKey)));
        Assert.assertEquals(100L,PAXEncodingState.readSequenceID(rowKey));
    }
*/

    @Test
    public void testHiveEncoding() throws Exception {
        ExecRow valueRow = new ValueRow(11);
        valueRow.setColumn(1,new SQLVarchar("fooo"));
        valueRow.setColumn(2,new SQLLongint(2l));
        valueRow.setColumn(3,new SQLInteger(1));
        valueRow.setColumn(4,new SQLTinyint((byte) 0));
        valueRow.setColumn(5,new SQLDate(new Date(System.currentTimeMillis())));
        valueRow.setColumn(6,new SQLTimestamp(Timestamp.valueOf("1962-09-01 03:23:34.234")));
        valueRow.setColumn(7,new SQLBoolean(true));
        valueRow.setColumn(8,new SQLBlob("1231".getBytes()));
        valueRow.setColumn(9,new UserType(new SQLChar("Splice Machine")));
        valueRow.setColumn(10,new SQLTime(new Time(System.currentTimeMillis())));
        valueRow.setColumn(11,new SQLDecimal(new BigDecimal(1)));


        StructType type = valueRow.createStructType(IntArrays.count(valueRow.size())); // Can we overload this method on execrow...
        TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(type.catalogString());
        SettableStructObjectInspector oi = (SettableStructObjectInspector) OrcStruct.createObjectInspector(typeInfo);
        OrcStruct orcStruct = (OrcStruct) oi.create();
        List<? extends StructField> structFields = oi.getAllStructFieldRefs();
        ByteArrayOutputStream baosInMemory = new ByteArrayOutputStream();
        DataOutputStream userDataStream = new DataOutputStream(baosInMemory);

        Writer writer = OrcFile.createWriter(new PAXBlockFileSystem(userDataStream),new Path("/DUMMY"), new Configuration(),oi,268435456, NONE, 262144, 10000);


        for (int i = 0; i < structFields.size(); i++) {
            // Fix this..
            oi.setStructFieldData(orcStruct, structFields.get(i),
                    valueRow.getColumn(i + 1).getHiveObject());
        }
        writer.addRow(orcStruct);

    }


}
