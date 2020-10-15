package com.splicemachine.si.impl;

import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.region.TxnFinder;
import com.splicemachine.si.impl.region.TxnTimeTraveler;
import com.splicemachine.utils.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class TimeTravelTest {

    private static Long toTs(String s) throws ParseException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
        Date parsedDate = dateFormat.parse(s);
        Timestamp timestamp = new java.sql.Timestamp(parsedDate.getTime());
        return timestamp.getTime();
    }

    class MockTxnFinder implements TxnFinder {


        List<SortedMap<Long, Long>> txnTimeline = new ArrayList<>();

        MockTxnFinder() {
            for(int i = 0; i < SIConstants.TRANSACTION_TABLE_BUCKET_COUNT; ++i) {
                txnTimeline.add(new TreeMap<>());
            }
        }

        MockTxnFinder AddTxn(int bucket, long txnId, String ts) throws ParseException {
            assert bucket >= 0 && bucket < SIConstants.TRANSACTION_TABLE_BUCKET_COUNT;
            txnTimeline.get(bucket).put(txnId, toTs(ts));
            return this;
        }

        @Override
        public Pair<Long, Long> find(byte bucket, byte[] begin, boolean reverse) throws IOException {
            SortedMap<Long, Long> m = txnTimeline.get(bucket);
            if(m.size() == 0) {
                return null;
            }
            if(begin == null) {
                if(reverse) {
                    return new Pair<>(m.lastKey(), m.get(m.lastKey()));
                } else {
                    return new Pair<>(m.firstKey(), m.get(m.firstKey()));
                }
            } else {
                long txnId = Bytes.toLong(Bytes.slice(begin, 1, begin.length-1));
                if(reverse) {
                    long prev = -1;
                    for(long k : m.keySet()) {
                        if(k <= txnId) {
                            prev = k;
                        } else {
                            return new Pair<>(prev, m.get(prev));
                        }
                    }
                } else {
                    for(long k : m.keySet()) {
                        if (k > txnId) {
                            return new Pair<>(k, m.get(k));
                        }
                    }
                }
            }
            return null;
        }
    }

    @Test
    public void testBinarySearch() throws Exception {
        MockTxnFinder txnFinder = new MockTxnFinder();
        txnFinder.AddTxn(0, 4999936, "2020-07-03 21:15:16.123")
                 .AddTxn(0, 5000960, "2020-07-03 21:16:16.123")
                 .AddTxn(0, 5001984, "2020-07-03 21:17:16.123")
                 .AddTxn(0, 5002752, "2020-07-03 21:17:18.123")
                 .AddTxn(0, 5003776, "2020-07-03 21:19:18.123")
                 .AddTxn(0, 5004800, "2020-07-03 21:20:18.123");
        TxnTimeTraveler timeTraveler = new TxnTimeTraveler(txnFinder);
        Assert.assertEquals(5002752, (long)timeTraveler.getTxAt(toTs("2020-07-03 21:17:30.123")).getFirst() - SIConstants.TRASANCTION_INCREMENT);
    }

    @Test
    public void testBinarySearchExtremeRanges() throws Exception {
        MockTxnFinder txnFinder = new MockTxnFinder();
        txnFinder.AddTxn(0, 4999936, "2019-10-10 21:00:00.000")
                 .AddTxn(0, 5000960, "2020-07-03 21:16:16.123")
                 .AddTxn(0, 5004800, "2050-07-03 21:20:18.123");
        TxnTimeTraveler timeTraveler = new TxnTimeTraveler(txnFinder);
        Assert.assertEquals(5000960, (long)timeTraveler.getTxAt(toTs("2020-07-03 21:17:30.123")).getFirst() - SIConstants.TRASANCTION_INCREMENT);
    }

    @Test
    public void testBinarySearchEmptyRange() throws Exception {
        MockTxnFinder txnFinder = new MockTxnFinder();
        txnFinder.AddTxn(0, 4999936, "2019-10-10 21:00:00.000")
                 .AddTxn(0, 5004800, "2050-07-03 21:20:18.123");
        TxnTimeTraveler timeTraveler = new TxnTimeTraveler(txnFinder);
        Assert.assertEquals(4999936, (long)timeTraveler.getTxAt(toTs("2020-07-03 21:17:30.123")).getFirst() - SIConstants.TRASANCTION_INCREMENT);
    }

    @Test
    public void testBinarySearchMultipleBuckets() throws Exception {
        MockTxnFinder txnFinder = new MockTxnFinder();
        txnFinder
                 .AddTxn(2, 4276480, "2020-07-03 21:27:16.123")
                 .AddTxn(2, 4276736, "2020-07-03 21:28:16.123")
                 .AddTxn(2, 4276992, "2020-07-03 21:29:16.123")
                 .AddTxn(2, 4277248, "2020-07-03 21:30:18.123")
                 .AddTxn(2, 4277504, "2020-07-03 21:31:18.123")
                 .AddTxn(2, 4277760, "2020-07-03 21:32:18.123");
        TxnTimeTraveler timeTraveler = new TxnTimeTraveler(txnFinder);
        Assert.assertEquals(4277504, (long)timeTraveler.getTxAt(toTs("2020-07-03 21:31:19.123")).getFirst() - SIConstants.TRASANCTION_INCREMENT);
    }
}