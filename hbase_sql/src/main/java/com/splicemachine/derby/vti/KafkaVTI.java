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

package com.splicemachine.derby.vti;

import com.splicemachine.db.iapi.error.ExceptionUtil;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.vti.VTICosting;
import com.splicemachine.db.vti.VTIEnvironment;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.impl.load.ImportUtils;
import com.splicemachine.derby.stream.function.FileFunction;
import com.splicemachine.derby.stream.function.StreamFileFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.stream.spark.ExternalizableDeserializer;
import com.splicemachine.derby.stream.spark.NativeSparkDataSet;
import com.splicemachine.derby.stream.spark.SparkDataSet;
import com.splicemachine.derby.vti.iapi.DatasetProvider;
import com.splicemachine.utils.kryo.ExternalizableSerializer;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerSerializer;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Externalizable;
import java.io.InputStream;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

public class KafkaVTI implements DatasetProvider, VTICosting{
    private static final Logger LOG = Logger.getLogger(KafkaVTI.class.getName());
    private OperationContext operationContext;
    public static final ThreadLocal<Dataset<Row>> datasetThreadLocal = new ThreadLocal();

    private String topicName;
    private String host;
    private String port;

    public KafkaVTI() {}

    public KafkaVTI(String topicName) {
        this.topicName = topicName;
    }

    @Override
    public DataSet<ExecRow> getDataSet(SpliceOperation op, DataSetProcessor dsp, ExecRow execRow) throws StandardException {
        DataSet<ExecRow> ds;
        if (op != null)
            operationContext = dsp.createOperationContext(op);
        else
            operationContext = dsp.createOperationContext((Activation)null);
        try
        {
            operationContext.pushScopeForOp("Read Kafka Topic");
            ds = dsp.readKafkaTopic(topicName, operationContext);
        }
        finally {
            operationContext.popScope();
        }
        return ds;
    }

    @Override
    public double getEstimatedRowCount(VTIEnvironment vtiEnvironment) throws SQLException {
        return 100000;
    }

    @Override
    public double getEstimatedCostPerInstantiation(VTIEnvironment vtiEnvironment) throws SQLException {
        return 100000;
    }

    @Override
    public boolean supportsMultipleInstantiations(VTIEnvironment vtiEnvironment) throws SQLException {
        return false;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw new SQLException("not supported");
    }

    @Override
    public OperationContext getOperationContext() {
        return operationContext;
    }
}
