/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.catalog;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.shared.common.sanity.SanityManager;

import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 2/25/15
 */
public class SYSTABLESTATISTICSRowFactory extends CatalogRowFactory {
    public static final String TABLENAME_STRING = "SYSTABLESTATS";
    public static final int SYSTABLESTATISTICS_COLUMN_COUNT= 11;
    public static final int CONGLOMID = 1;
    public static final int PARTITIONID = 2; // GLOBAL
    public static final int TIMESTAMP = 3;
    public static final int STALENESS = 4;
    public static final int INPROGRESS = 5;
    public static final int ROWCOUNT = 6;
    public static final int PARTITION_SIZE = 7;
    public static final int MEANROWWIDTH= 8;
    public static final int NUMBEROFPARTITIONS = 9;
    public static final int STATSTYPE = 10;
    public static final int SAMPLEFRACTION = 11;

    /* Following are values supported in statsType */
    public static final int REGULAR_NONMERGED_STATS = 0;
    public static final int SAMPLE_NONMERGED_STATS = 1;
    public static final int REGULAR_MERGED_STATS = 2;
    public static final int SAMPLE_MERGED_STATS = 3;
    public static final int FAKE_MERGED_STATS = 4;


    protected static final int		SYSTABLESTATISTICS_INDEX1_ID = 0;
    protected static final int		SYSTABLESTATISTICS_INDEX2_ID = 1;
    protected static final int		SYSTABLESTATISTICS_INDEX3_ID = 2;



    private String[] uuids = {
            "08264012-014b-c29b-a826-000003009390",
            "0826401a-014b-c29b-a826-000003009390",
            "08264014-014b-c29b-a826-000003009390",
            "08264016-014b-c29b-a826-000003009390",
            "08264018-014b-c29b-a826-000003009390"
    };

    private	static	final	boolean[]	uniqueness = {
            true,
            false,
            false
    };

    private static final int[][] indexColumnPositions = {
                    {CONGLOMID, PARTITIONID,TIMESTAMP},
                    {CONGLOMID, PARTITIONID},
                    {CONGLOMID},
            };

    public SYSTABLESTATISTICSRowFactory(UUIDFactory uuidf,ExecutionFactory ef,DataValueFactory dvf,DataDictionary dd) {
        super(uuidf, ef, dvf, dd);
        initInfo(SYSTABLESTATISTICS_COLUMN_COUNT,TABLENAME_STRING,indexColumnPositions,uniqueness,uuids);
    }

    @Override
    public ExecRow makeRow(boolean latestVersion, TupleDescriptor td, TupleDescriptor parent) throws StandardException {
        long conglomId = 0;
        String partitionId = null;
        long timestamp = 0;
        boolean staleness = false;
        boolean inProgress = false;
        long rowCount = 0;
        long partitionSize = 0;
        int meanRowWidth=0;
        long numberOfPartitions = 1L;
        int statsType = REGULAR_NONMERGED_STATS;
        double sampleFraction = 0.0d;

        if(td!=null){
            if (!(td instanceof PartitionStatisticsDescriptor))
                throw new RuntimeException("Unexpected TupleDescriptor " + td.getClass().getName());

            PartitionStatisticsDescriptor tsd = (PartitionStatisticsDescriptor)td;
            conglomId = tsd.getConglomerateId();
            partitionId = tsd.getPartitionId();
            timestamp  = tsd.getTimestamp();
            staleness = tsd.isStale();
            inProgress= tsd.isInProgress();
            rowCount = tsd.getRowCount();
            partitionSize = tsd.getPartitionSize();
            meanRowWidth = tsd.getMeanRowWidth();
            numberOfPartitions = tsd.getNumberOfPartitions();
            statsType = tsd.getStatsType();
            sampleFraction = tsd.getSampleFraction();
        }

        ExecRow row = getExecutionFactory().getValueRow(SYSTABLESTATISTICS_COLUMN_COUNT);
        row.setColumn(CONGLOMID,new SQLLongint(conglomId));
        row.setColumn(PARTITIONID,new SQLVarchar(partitionId));
        row.setColumn(TIMESTAMP,new SQLTimestamp(new Timestamp(timestamp)));
        row.setColumn(STALENESS,new SQLBoolean(staleness));
        row.setColumn(INPROGRESS,new SQLBoolean(inProgress));
        row.setColumn(ROWCOUNT,new SQLLongint(rowCount));
        row.setColumn(PARTITION_SIZE,new SQLLongint(partitionSize));
        row.setColumn(MEANROWWIDTH,new SQLInteger(meanRowWidth));
        row.setColumn(NUMBEROFPARTITIONS,new SQLLongint(numberOfPartitions));
        row.setColumn(STATSTYPE, new SQLInteger(statsType));
        row.setColumn(SAMPLEFRACTION, new SQLDouble(sampleFraction));
        return row;
    }

    @Override
    public TupleDescriptor buildDescriptor(ExecRow row, TupleDescriptor parentTuple, DataDictionary dataDictionary) throws StandardException {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT( row.nColumns() ==SYSTABLESTATISTICS_COLUMN_COUNT,
                    "Wrong number of columns for a STATEMENTHISTORY row");
        }

        DataValueDescriptor col = row.getColumn(CONGLOMID);
        long conglomId = col.getLong();
        col = row.getColumn(PARTITIONID);
        String partitionId = col.getString();
        col = row.getColumn(TIMESTAMP);
        Timestamp timestamp = col.getTimestamp(null);
        col = row.getColumn(STALENESS);
        boolean isStale = col.getBoolean();
        col = row.getColumn(INPROGRESS);
        boolean inProgress = col.getBoolean();
        col = row.getColumn(ROWCOUNT);
        long rowCount = col.getLong();
        col = row.getColumn(PARTITION_SIZE);
        long partitionSize = col.getLong();
        col = row.getColumn(MEANROWWIDTH);
        int rowWidth = col.getInt();
        col = row.getColumn(NUMBEROFPARTITIONS);
        long numberOfPartitions = col.getLong();
        col = row.getColumn(STATSTYPE);
        int statsType = col.getInt();
        col = row.getColumn(SAMPLEFRACTION);
        double sampleFaction = col.getDouble();

        return new PartitionStatisticsDescriptor(conglomId,
                partitionId,
                timestamp.getTime(),
                isStale,
                inProgress,
                rowCount,
                partitionSize,
                rowWidth,
                numberOfPartitions,
                statsType,
                sampleFaction);
    }

    @Override
    public SystemColumn[] buildColumnList() throws StandardException {
        return new SystemColumn[]{
                SystemColumnImpl.getColumn("CONGLOMERATEID", Types.BIGINT,false),
                SystemColumnImpl.getColumn("PARTITIONID",Types.VARCHAR,false),
                SystemColumnImpl.getColumn("LAST_UPDATED",Types.TIMESTAMP,false),
                SystemColumnImpl.getColumn("IS_STALE",Types.BOOLEAN,false),
                SystemColumnImpl.getColumn("IN_PROGRESS", Types.BOOLEAN, false),
                SystemColumnImpl.getColumn("ROWCOUNT",Types.BIGINT,true),
                SystemColumnImpl.getColumn("PARTITION_SIZE",Types.BIGINT,true),
                SystemColumnImpl.getColumn("MEANROWWIDTH",Types.INTEGER,true),
                SystemColumnImpl.getColumn("NUMPARTITIONS",Types.BIGINT,true),
                SystemColumnImpl.getColumn("STATSTYPE", Types.INTEGER, true),
                SystemColumnImpl.getColumn("SAMPLEFRACTION",Types.DOUBLE, true)
        };
    }

    public List<ColumnDescriptor[]> getViewColumns(TableDescriptor view, UUID viewId) throws StandardException {
        DataTypeDescriptor varcharType = DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR);
        DataTypeDescriptor longType = DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT);
        DataTypeDescriptor intType = DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER);
        DataTypeDescriptor doubleType = DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.DOUBLE);

        List<ColumnDescriptor[]> cdsl = new ArrayList<>();
        cdsl.add(
               new ColumnDescriptor[]{
                new ColumnDescriptor("SCHEMANAME"               ,1,1,varcharType,null,null,view,viewId,0,0,0),
                new ColumnDescriptor("TABLENAME"                ,2,2,varcharType,null,null,view,viewId,0,0,1),
                new ColumnDescriptor("CONGLOMERATENAME"         ,3,3,varcharType,null,null,view,viewId,0,0,2),
                new ColumnDescriptor("TOTAL_ROW_COUNT"          ,4,4,longType,null,null,view,viewId,0,0,3),
                new ColumnDescriptor("AVG_ROW_COUNT"            ,5,5,longType,null,null,view,viewId,0,0,4),
                new ColumnDescriptor("TOTAL_SIZE"               ,6,6,longType,null,null,view,viewId,0,0,5),
                new ColumnDescriptor("NUM_PARTITIONS"           ,7,7,longType,null,null,view,viewId,0,0,6),
                new ColumnDescriptor("AVG_PARTITION_SIZE"       ,8,8,longType,null,null,view,viewId,0,0,7),
                new ColumnDescriptor("ROW_WIDTH"                ,9,9,longType,null,null,view,viewId,0,0,8),
                new ColumnDescriptor("STATS_TYPE"               ,10,10,intType,null,null,view,viewId,0,0,9),
                new ColumnDescriptor("SAMPLE_FRACTION"          ,11,11,doubleType,null,null,view,viewId,0,0,10)
        });

        return cdsl;
    }

    public static final String STATS_VIEW_SQL = "create view systablestatistics as select " +
            "s.schemaname" +
            ",t.tablename" + // 1
            ",c.conglomeratename" + //2
            ",sum(ts.rowCount) as TOTAL_ROW_COUNT" +  //3
            ",sum(ts.rowCount)/sum(ts.numPartitions) as AVG_ROW_COUNT" +      //4
            ",sum(ts.partition_size) as TOTAL_SIZE" + //5
            ",sum(ts.numPartitions) as NUM_PARTITIONS" + //6
            ",sum(ts.partition_size)/sum(ts.numPartitions) as AVG_PARTITION_SIZE" + //7
            ",max(ts.meanrowWidth) as ROW_WIDTH" + //8
            ",ts.statsType as STATS_TYPE" + //9
            ",min(ts.sampleFraction) as SAMPLE_FRACTION" + //10
            " from " +
            "sys.systables t" +
            ",sysvw.sysschemasview s" +
            ",sys.sysconglomerates c" +
            ",sys.systablestats ts" +
            " where " +
            "t.tableid = c.tableid " +
            "and c.conglomeratenumber = ts.conglomerateid " +
            "and t.schemaid = s.schemaid " +
            " group by " +
            "s.schemaname" +
            ",t.tablename"+
            ",c.conglomeratename" +
            ",ts.statsType";

    public static void main(String...args) {
        System.out.println(STATS_VIEW_SQL);
    }




}
