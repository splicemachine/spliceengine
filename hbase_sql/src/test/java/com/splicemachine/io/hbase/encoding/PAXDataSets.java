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

import com.splicemachine.access.impl.data.UnsafeRecord;
import com.splicemachine.access.impl.data.UnsafeRecordUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.constants.SIConstants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.spark.sql.types.Decimal;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

/**
 * Created by jleach on 9/28/17.
 */
public class PAXDataSets {

    public static Iterator<Cell> getTestDataSet() throws StandardException, java.text.ParseException {
        ExecRow execRow = new ValueRow(6);
        execRow.setRowArray(new DataValueDescriptor[]{
                new SQLVarchar(),
                new SQLInteger(),
                new SQLLongint(),
                new SQLDate(),
                new SQLDecimal(null,10,2),
                new SQLTimestamp()
        });


        UnsafeRecord record = new UnsafeRecord(
                "rowkey123".getBytes(),
                2L,
                new byte[UnsafeRecordUtils.calculateFixedRecordSize(6)],
                0l,true);

        record.setVersion(1);
        record.setHasTombstone(false);
        record.setTxnId1(123456l);
        record.setEffectiveTimestamp(123456l);
        record.setNumberOfColumns(6);

        DateFormat df = new SimpleDateFormat("MM-dd-yyyy");
        List<Cell> keyValues = new ArrayList<>(1500);
        for (int i = 0; i< 220; i++) {
            record.setKey(Bytes.toBytes(i));
            record.setData(new DataValueDescriptor[]{
                    new SQLVarchar("-" + i),
                    new SQLInteger(i),
                    new SQLLongint(i),
                    new SQLDate(new java.sql.Date(df.parse("02-04-2015").getTime())),
                    new SQLDecimal(new BigDecimal(i), 10, 2),
                    new SQLTimestamp(new Timestamp(System.currentTimeMillis()))});
//                    new SQLTimestamp()});
            keyValues.add(new KeyValue(record.getKey(), SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.PACKED_COLUMN_BYTES,record.getValue()));
        }
        return keyValues.iterator();
    }

    public static Iterator<Cell> largeTestDataSet() throws StandardException, java.text.ParseException {
        ExecRow execRow = new ValueRow(6);
        execRow.setRowArray(new DataValueDescriptor[]{
                new SQLVarchar(),
                new SQLInteger(),
                new SQLLongint(),
                new SQLDate(),
                new SQLDecimal(null,10,2),
                new SQLTimestamp()
        });


        UnsafeRecord record = new UnsafeRecord(
                "rowkey123".getBytes(),
                2L,
                new byte[UnsafeRecordUtils.calculateFixedRecordSize(6)],
                0l,true);

        record.setVersion(1);
        record.setHasTombstone(false);
        record.setTxnId1(123456l);
        record.setEffectiveTimestamp(123456l);
        record.setNumberOfColumns(6);

        DateFormat df = new SimpleDateFormat("MM-dd-yyyy");
        List<Cell> keyValues = new ArrayList<>(1500);
        for (int i = 0; i< 65536; i++) {
            record.setKey(Bytes.toBytes(i));
            record.setData(new DataValueDescriptor[]{
                    new SQLVarchar("-" + i),
                    new SQLInteger(i),
                    new SQLLongint(i),
                    new SQLDate(new java.sql.Date(df.parse("02-04-2015").getTime())),
                    new SQLDecimal(new BigDecimal(i), 10, 2),
                    new SQLTimestamp(new Timestamp(System.currentTimeMillis()))});
//                    new SQLTimestamp()});
            keyValues.add(new KeyValue(record.getKey(), SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.PACKED_COLUMN_BYTES,record.getValue()));
        }
        return keyValues.iterator();
    }

}
