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

import com.splicemachine.art.SimpleART;
import com.splicemachine.art.node.Base;
import com.splicemachine.art.tree.ART;
import com.splicemachine.si.constants.SIConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.SettableSequenceId;
import org.apache.hadoop.hbase.regionserver.DefaultMemStore;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.List;
import java.util.stream.IntStream;


/**
 * Created by jleach on 11/21/17.
 */

public class PAXSmokeTest {

    @Test
    public void smokeTest() throws Exception {
        for (int j = 0; j< 1000; j++) {
            Random random = new Random();
            DefaultMemStore memStore = new DefaultMemStore(new Configuration(), KeyValue.COMPARATOR);
            ART art = new SimpleART();
            for (int i = 0; i < 100000; i++) {
                byte[] bytes = new byte[random.nextInt(40) + 2];
                random.nextBytes(bytes);
                removeEncodingIssues(bytes); // removes 0x00
                bytes[bytes.length-1] = 0x00; // emulates Splice Machine key generation
                KeyValue keyValue = new KeyValue(bytes, SIConstants.DEFAULT_FAMILY_BYTES, SIConstants.PACKED_COLUMN_BYTES, random.nextInt(20) + 1, org.apache.hadoop.hbase.KeyValue.Type.Put, "1".getBytes());
                ((SettableSequenceId) keyValue).setSequenceId(i);
                memStore.add(keyValue);
                byte[] artRow = PAXEncodingState.genRowKey(keyValue);
                art.insert(artRow, keyValue.getValueArray());
                Assert.assertTrue(Bytes.equals(artRow,0,artRow.length-18,
                        keyValue.getRowArray(),keyValue.getRowOffset(),keyValue.getRowLength()));
               // System.out.println("FrontEnd");
               // System.out.println(Base.toHex(artRow,0,artRow.length-18) + "\n" +  Base.toHex(keyValue.getRowArray(),keyValue.getRowOffset(),keyValue.getRowLength()));
            }
            //System.out.println(art.debugString(false));
            Iterator<ByteBuffer[]> buffers = art.getRootIterator();
            KeyValueScanner kvs = memStore.snapshot().getScanner();
            while (buffers.hasNext()) {
                byte[] artRow = buffers.next()[0].array();
                Cell memCell = kvs.next();
                //System.out.println("Backend");
                //System.out.println(Base.toHex(artRow,0,artRow.length-18) + "\n" +  Base.toHex(memCell.getRowArray(),memCell.getRowOffset(),memCell.getRowLength()));
                Assert.assertTrue("arrays not equal\n"+ Base.toHex(artRow,0,artRow.length-18) + "\n" +  Base.toHex(memCell.getRowArray(),memCell.getRowOffset(),memCell.getRowLength()) + "\n",
                        Bytes.equals(artRow,0,artRow.length-18,
                        memCell.getRowArray(),memCell.getRowOffset(),memCell.getRowLength()));
            }
            System.out.println("Completed -> " + j);
        }
//        List<KeyValueScanner> scanners = memStore.getScanners(0);
//        Cell kv;
//        scanners.get(0).getNextIndexedKey();
//        while ( (kv = scanners.get(0).next()) != null )
//            System.out.println(scanners.get(0).next());
    }


    private void removeEncodingIssues(byte[] random) {
        for(int i=0;i<random.length-1;i++){
            if (random[i] == 0x00)
                random[i] = (byte)(random[i]+1);
        }



    }

}
