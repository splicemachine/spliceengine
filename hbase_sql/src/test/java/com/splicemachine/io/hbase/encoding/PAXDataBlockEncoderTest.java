package com.splicemachine.io.hbase.encoding;

import com.splicemachine.access.impl.data.UnsafeRecord;

import java.util.*;

import com.splicemachine.art.node.Base;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.impl.SpliceQuery;
import com.splicemachine.si.impl.server.RedoTransactor;
import com.splicemachine.storage.HCell;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoder;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.encoding.HFileBlockDefaultEncodingContext;
import org.apache.hadoop.hbase.io.encoding.HFileBlockEncodingContext;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.NoOpDataBlockEncoder;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;


/**
 * Created by jleach on 9/27/17.
 */
@Ignore
public class PAXDataBlockEncoderTest {
    private static Logger LOG = Logger.getLogger(PAXDataBlockEncoderTest.class);

/*    @Test
    public void testWriteDescendingLongWithRandomSortedLongs() {
        byte[] prior = new byte[9];
        byte[] current = new byte[9];
        Random random = new Random();
        List<Long> randomLongs = new ArrayList<>(1000);
        for (int i = 0; i< 1000; i++) {
            randomLongs.add(random.nextLong());
        }
        Collections.sort(randomLongs);
        for (int i = 0; i< 1000; i++) {
            if (i != 0L) {
                PAXEncodingState.writeDescendingLong(current,16,randomLongs.get(i).longValue());
                Assert.assertTrue("i->"+i, (Bytes.compareBytes(true,prior,current)> 0));
            }
            PAXEncodingState.writeDescendingLong(prior,16,randomLongs.get(i).longValue());
        }
    }
*/
    @Test
    public void testSeek() {
        String seekKey = "0315FD0185CC5001";
        String genSeekKey = "0315FD0185CC500100000000000000000E7FFFFFFFFFFFFFFF";
        String actualKey = "0315FD0185CC50017FFFFFFFFFFFFFFE047FFFFFFFFFFFFFFA";
        int compare = Bytes.compareBytes(false, Base.fromHex(genSeekKey),Bytes.fromHex(actualKey));
        System.out.println("Compare " + compare);
    }


    @Test
    public void testNoOpDataBlockEncoder() throws Exception {
        NoOpDataBlockEncoder encoder = NoOpDataBlockEncoder.INSTANCE;
        HFileContext meta = new HFileContextBuilder()
                .withHBaseCheckSum(false)
                .withIncludesMvcc(false)
                .withIncludesTags(false)
                .withCompression(Compression.Algorithm.NONE).build();
        HFileBlockEncodingContext blkEncodingCtx = new HFileBlockDefaultEncodingContext(
                DataBlockEncoding.NONE, new byte[0], meta);
        ByteArrayOutputStream baosInMemory = new ByteArrayOutputStream();
        DataOutputStream userDataStream = new DataOutputStream(baosInMemory);
        encoder.startBlockEncoding(blkEncodingCtx, userDataStream);
        Iterator<Cell> cells = PAXDataSets.getTestDataSet();
        Cell first = null;

        while (cells.hasNext()) {
            Cell next = cells.next();
            if (first == null)
                first = next;
            encoder.encode(next, blkEncodingCtx, userDataStream);
        }
        encoder.endBlockEncoding(blkEncodingCtx, userDataStream, new byte[]{}, BlockType.DATA);
    }


    @Test
    public void testSeekBeforeWithFixedData() throws Exception {
        ExecRow execRow = createExecRow();
        FormatableBitSet fbs = new FormatableBitSet(6);
        fbs.setAll();
        SpliceQuery spliceQuery = new SpliceQuery(execRow,fbs);
        RedoTransactor.queryContext.set(spliceQuery);
        PAXDataBlockEncoder encoder = new PAXDataBlockEncoder(execRow);
        HFileBlockEncodingContext blkEncodingCtx = createHFileBlockEncodingContext();
        ByteArrayOutputStream baosInMemory = new ByteArrayOutputStream();
        DataOutputStream userDataStream = new DataOutputStream(baosInMemory);
        encoder.startBlockEncoding(blkEncodingCtx, userDataStream);
        Iterator<Cell> cells = PAXDataSets.getTestDataSet();
        Cell first = null;
        while (cells.hasNext()) {
            Cell next = cells.next();
            if (first == null)
                first = next;
            encoder.encode(next, blkEncodingCtx, userDataStream);
        }
        encoder.endBlockEncoding(blkEncodingCtx, userDataStream, new byte[]{});
        DataBlockEncoder.EncodedSeeker seeker = encoder.createSeeker(null,
                encoder.newDataBlockDecodingContext(blkEncodingCtx.getHFileContext()));

        byte[] onDiskBytes = baosInMemory.toByteArray();
        ByteBuffer readBuffer = ByteBuffer.wrap(onDiskBytes);
        readBuffer.position(2);
        seeker.setCurrentBuffer(readBuffer);

        // Seek before the first keyvalue;
        seeker.seekToKeyInBlock(first, true);
        HCell cell = new HCell();
        int[] columns = IntArrays.count(execRow.size());
        UnsafeRecord unsafeRecord = new UnsafeRecord();
        int i =0;
        while (seeker.next()) {
            cell.set(seeker.getKeyValue());
            unsafeRecord.wrap(cell);
            unsafeRecord.getData(columns,execRow);
            unsafeRecord = new UnsafeRecord();
            cell.set(seeker.getKeyValue());
            unsafeRecord.wrap(cell);
            unsafeRecord.getData(columns,execRow);
            if (LOG.isDebugEnabled())
                SpliceLogUtils.debug(LOG,"RowKey=%s, ExecRow=%s", Bytes.toHex(unsafeRecord.getKey()),execRow);
            i++;
        }
        Assert.assertEquals(219,i);
    }


    @Test
    public void testLargeWrite() throws Exception {
        ExecRow execRow = createExecRow();
        FormatableBitSet fbs = new FormatableBitSet(6);
        fbs.setAll();
        SpliceQuery spliceQuery = new SpliceQuery(execRow,fbs);
        RedoTransactor.queryContext.set(spliceQuery);
        PAXDataBlockEncoder encoder = new PAXDataBlockEncoder(execRow);
        HFileBlockEncodingContext blkEncodingCtx = createHFileBlockEncodingContext();
        ByteArrayOutputStream baosInMemory = new ByteArrayOutputStream();
        DataOutputStream userDataStream = new DataOutputStream(baosInMemory);
        encoder.startBlockEncoding(blkEncodingCtx, userDataStream);
        Iterator<Cell> cells = PAXDataSets.largeTestDataSet();
        Cell first = null;
        int j = 0;
        while (cells.hasNext()) {
            Cell next = cells.next();
            if (first == null)
                first = next;
            encoder.encode(next, blkEncodingCtx, userDataStream);
            j++;
//            if (j> 15000)
//                break;

        }
        encoder.endBlockEncoding(blkEncodingCtx, userDataStream, new byte[]{});
        DataBlockEncoder.EncodedSeeker seeker = encoder.createSeeker(null,
                encoder.newDataBlockDecodingContext(blkEncodingCtx.getHFileContext()));

        byte[] onDiskBytes = baosInMemory.toByteArray();
        ByteBuffer readBuffer = ByteBuffer.wrap(onDiskBytes);
        readBuffer.position(2);
        seeker.setCurrentBuffer(readBuffer);

        // Seek before the first keyvalue;
        seeker.seekToKeyInBlock(first, true);
        HCell cell = new HCell();
        int[] columns = IntArrays.count(execRow.size());
        UnsafeRecord unsafeRecord = new UnsafeRecord();
        int i =1;
        while (seeker.next()) {
            cell.set(seeker.getKeyValue());
            unsafeRecord.wrap(cell);
            unsafeRecord.getData(columns,execRow);
            unsafeRecord = new UnsafeRecord();
            cell.set(seeker.getKeyValue());
            unsafeRecord.wrap(cell);
            unsafeRecord.getData(columns,execRow);
            if (i == 254) {
                System.out.println("Foo");
            }
            Assert.assertEquals(execRow.getColumn(2).getInt(),i);
            i++;
        }
        Assert.assertEquals(65536,i);
    }

    @Test
    public void testFixOffset() throws Exception {
        ExecRow execRow = createExecRow();
        FormatableBitSet fbs = new FormatableBitSet(6);
        fbs.setAll();
        SpliceQuery spliceQuery = new SpliceQuery(execRow,fbs);
        RedoTransactor.queryContext.set(spliceQuery);
        PAXDataBlockEncoder encoder = new PAXDataBlockEncoder(execRow);
        HFileBlockEncodingContext blkEncodingCtx = createHFileBlockEncodingContext();
        ByteArrayOutputStream baosInMemory = new ByteArrayOutputStream();
        DataOutputStream userDataStream = new DataOutputStream(baosInMemory);
        encoder.startBlockEncoding(blkEncodingCtx, userDataStream);
        Iterator<Cell> cells = PAXDataSets.getTestDataSet();
        Cell first = null;
        int i = 0;
        while (cells.hasNext()) {
            Cell next = cells.next();
            if (i == 31)
                first = next;
            i++;
            encoder.encode(next, blkEncodingCtx, userDataStream);
        }
        encoder.endBlockEncoding(blkEncodingCtx, userDataStream, new byte[]{});
        DataBlockEncoder.EncodedSeeker seeker = encoder.createSeeker(null,
                encoder.newDataBlockDecodingContext(blkEncodingCtx.getHFileContext()));

        byte[] onDiskBytes = baosInMemory.toByteArray();
        ByteBuffer readBuffer = ByteBuffer.wrap(onDiskBytes);
        readBuffer.position(2);
        seeker.setCurrentBuffer(readBuffer);

        // Seek before the first keyvalue;
        seeker.seekToKeyInBlock(first, true);
        HCell cell = new HCell();
        int[] columns = IntArrays.count(execRow.size());
        UnsafeRecord unsafeRecord = new UnsafeRecord();
        i =0;
        while (seeker.next()) {
            cell.set(seeker.getKeyValue());
            unsafeRecord.wrap(cell);
            unsafeRecord.getData(columns,execRow);
            unsafeRecord = new UnsafeRecord();
            cell.set(seeker.getKeyValue());
            unsafeRecord.wrap(cell);
            unsafeRecord.getData(columns,execRow);
            if (LOG.isDebugEnabled())
                SpliceLogUtils.debug(LOG,"RowKey=%s, ExecRow=%s", Bytes.toHex(unsafeRecord.getKey()),execRow);
            i++;
        }
        Assert.assertEquals(188,i);
    }

    private ExecRow createExecRow() throws StandardException {
        ExecRow execRow = new ValueRow(6);
        execRow.setRowArray(new DataValueDescriptor[]{
                new SQLVarchar(),
                new SQLInteger(),
                new SQLLongint(),
                new SQLDate(),
                new SQLDecimal(null,10,2),
                new SQLTimestamp()
        });
        return execRow;
    }

    public HFileBlockEncodingContext createHFileBlockEncodingContext() {
        HFileContext meta = new HFileContextBuilder()
                .withHBaseCheckSum(false)
                .withIncludesMvcc(false)
                .withIncludesTags(false)
                .withCompression(Compression.Algorithm.NONE).build();
        return new HFileBlockDefaultEncodingContext(
                DataBlockEncoding.PREFIX_TREE, new byte[0], meta);
    }

    @Test
    public void foo() throws Exception{
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        DataOutputStream daos = new DataOutputStream(baos);
        daos.writeInt(65536);
        byte[] onDiskBytes = baos.toByteArray();
        ByteBuffer readBuffer = ByteBuffer.wrap(onDiskBytes);
        System.out.println(readBuffer.getInt());


    }


}
