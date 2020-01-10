/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
package com.splicemachine.derby.stream.spark;

import com.clearspring.analytics.util.Lists;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptorList;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.stream.function.*;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.protobuf.ProtoUtil;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.Partition;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.*;

/**
 * Created by jyuan on 5/31/17.
 */
public class BulkDataSetWriter  {

    protected DataSet dataSet;
    protected OperationContext operationContext;
    protected long heapConglom;
    protected TxnView txn;
    protected byte[] token;

    protected static final Logger LOG=Logger.getLogger(BulkDataSetWriter.class);


    public BulkDataSetWriter() {}

    public BulkDataSetWriter(DataSet dataset, OperationContext operationContext, long heapConglom, TxnView txn, byte[] token) {
        this.dataSet = dataset;
        this.operationContext = operationContext;
        this.heapConglom = heapConglom;
        this.txn = txn;
        this.token = token;
    }

    /**
     * Get actual partition boundaries for each table and index
     * @param congloms
     * @return
     */
    public static List<BulkImportPartition> getBulkImportPartitions(
            List<Long> congloms,
            String bulkImportDirectory) throws StandardException {

        try {
            // Create bulk import directory if it does not exist
            Path bulkImportPath = new Path(bulkImportDirectory);

            List<BulkImportPartition> bulkImportPartitions = Lists.newArrayList();
            for (Long conglom : congloms) {
                Path tablePath = new Path(bulkImportPath, conglom.toString());
                SIDriver driver = SIDriver.driver();
                try (PartitionAdmin pa = driver.getTableFactory().getAdmin()) {
                    Iterable<? extends Partition> partitions = pa.allPartitions(conglom.toString());

                    if (LOG.isDebugEnabled()) {
                        SpliceLogUtils.debug(LOG, "partition information for table %d", conglom);
                    }
                    int count = 0;
                    for (Partition partition : partitions) {
                        String regionName = partition.getName();
                        byte[] startKey = partition.getStartKey();
                        byte[] endKey = partition.getEndKey();
                        Path regionPath = getRandomFilename(tablePath);
                        Path familyPath = new Path(regionPath, "V");
                        bulkImportPartitions.add(new BulkImportPartition(conglom, regionName, startKey, endKey, familyPath.toString()));
                        count++;
                        if (LOG.isDebugEnabled()) {
                            SpliceLogUtils.debug(LOG, "start key: %s", Bytes.toHex(startKey));
                            SpliceLogUtils.debug(LOG, "end key: %s", Bytes.toHex(endKey));
                            SpliceLogUtils.debug(LOG, "path = %s", familyPath.toString());
                        }
                    }
                    if (LOG.isDebugEnabled()) {
                        SpliceLogUtils.debug(LOG, "number of partition: %d", count);
                    }
                }
            }
            Collections.sort(bulkImportPartitions, BulkImportUtils.getSortComparator());
            return bulkImportPartitions;
        } catch (IOException e) {
            throw StandardException.plainWrapException(e);
        }
    }

    /**
     * Generate a file name
     * @param dir
     * @return
     * @throws IOException
     */

    public static Path getRandomFilename(final Path dir)
            throws IOException{
        return new Path(dir, java.util.UUID.randomUUID().toString().replaceAll("-",""));
    }

    protected void partitionUsingRDDSortUsingDataFrame(List<BulkImportPartition> bulkImportPartitions,
                                                       DataSet rowAndIndexes,
                                                       HFileGenerationFunction hfileGenerationFunction) {

        // Create a data frame for main table and index key/values, and sort data
        PairDataSet pairDataSet = rowAndIndexes.keyBy(new BulkImportKeyerFunction());
        int taskPerRegion = HConfiguration.getConfiguration().getBulkImportTasksPerRegion();
        JavaRDD partitionedJavaRDD =
                ((SparkPairDataSet)pairDataSet).rdd
                        .partitionBy(new BulkImportPartitioner(bulkImportPartitions, taskPerRegion)).values();

        JavaRDD<Row> javaRowRdd = partitionedJavaRDD.map(new HBaseBulkImportRowToSparkRowFunction());

        SparkSession sparkSession = SpliceSpark.getSession();
        StructType schema = createSchema();
        Dataset<Row> rowAndIndexesDataFrame =
                sparkSession.createDataFrame(javaRowRdd, schema);

        // Sort with each partition using row key
        Dataset partitionAndSorted =  rowAndIndexesDataFrame
                .sortWithinPartitions(new Column("key"));

        Dataset<String> hFileSet = partitionAndSorted.mapPartitions(hfileGenerationFunction, Encoders.STRING());

        Object files = hFileSet.collect();
    }

    private StructType createSchema() {
        List<StructField> fields = new ArrayList<>();
        StructField field = DataTypes.createStructField("conglomerateId", DataTypes.LongType, true);
        fields.add(field);

        field = DataTypes.createStructField("key", DataTypes.StringType, true);
        fields.add(field);

        field = DataTypes.createStructField("value", DataTypes.BinaryType, true);
        fields.add(field);

        StructType schema = DataTypes.createStructType(fields);

        return schema;
    }

    /**
     *
     * @param bulkImportPartitions
     * @throws StandardException
     */
    protected void bulkLoad(List<BulkImportPartition> bulkImportPartitions, String bulkImportDirectory, String prefix) throws StandardException{
        SConfiguration sConfiguration = HConfiguration.getConfiguration();
        int regionsPerTask = sConfiguration.getRegionToLoadPerTask();
        int numTasks = Math.max(bulkImportPartitions.size()/regionsPerTask, 1);
        SpliceSpark.pushScope(prefix + " Load HFiles");
        SpliceSpark.getContext().parallelize(bulkImportPartitions, numTasks)
                .foreachPartition(new BulkImportFunction(bulkImportDirectory, token));
        SpliceSpark.popScope();
    }

    protected void getAllConglomerates(List<Long> allCongloms, ArrayList<DDLMessage.TentativeIndex> tentativeIndexList)  throws StandardException{
        Activation activation = operationContext.getActivation();
        DataDictionary dd = activation.getLanguageConnectionContext().getDataDictionary();
        ConglomerateDescriptor cd = dd.getConglomerateDescriptor(heapConglom);
        TableDescriptor td = dd.getTableDescriptor(cd.getTableID());
        ConglomerateDescriptorList list = td.getConglomerateDescriptorList();

        allCongloms.add(td.getHeapConglomerateId());

        for (ConglomerateDescriptor searchCD :list) {
            if (searchCD.isIndex() && !searchCD.isPrimaryKey()) {
                DDLMessage.DDLChange ddlChange = ProtoUtil.createTentativeIndexChange(txn.getTxnId(),
                        activation.getLanguageConnectionContext(),
                        td.getHeapConglomerateId(), searchCD.getConglomerateNumber(),
                        td, searchCD.getIndexDescriptor(),td.getDefaultValue(searchCD.getIndexDescriptor().baseColumnPositions()[0]));
                tentativeIndexList.add(ddlChange.getTentativeIndex());
                allCongloms.add(searchCD.getConglomerateNumber());
            }
        }
    }
}
