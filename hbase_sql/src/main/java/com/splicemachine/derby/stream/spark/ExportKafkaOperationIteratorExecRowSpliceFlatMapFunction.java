package com.splicemachine.derby.stream.spark;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.export.ExportKafkaOperation;
import com.splicemachine.derby.stream.function.SpliceFlatMapFunction;
import com.splicemachine.derby.stream.iapi.OperationContext;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.spark.TaskContext;
import org.apache.spark.TaskKilledException;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;

class ExportKafkaOperationIteratorExecRowSpliceFlatMapFunction extends SpliceFlatMapFunction<ExportKafkaOperation, Iterator<Integer>, ExecRow> {
    private String topicName;

    public ExportKafkaOperationIteratorExecRowSpliceFlatMapFunction() {
    }

    public ExportKafkaOperationIteratorExecRowSpliceFlatMapFunction(OperationContext context, String topicName) {
        super(context);
        this.topicName = topicName;
    }

    @Override
    public Iterator<ExecRow> call(Iterator<Integer> partitions) throws Exception {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  "localhost:" + 9092);

        String group_id = "spark-consumer-"+UUID.randomUUID();
        props.put(ConsumerConfig.GROUP_ID_CONFIG,group_id);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ExternalizableDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //For Debug
        System.out.println("System print group_id:"+group_id);

        KafkaConsumer<Integer, Externalizable> consumer = new KafkaConsumer<Integer, Externalizable>(props);
        int partition = partitions.next();
        consumer.assign(Arrays.asList(new TopicPartition(topicName, partition)));

        return new Iterator<ExecRow>() {
            ConsumerRecords<Integer, Externalizable> records = null;
            Iterator<ConsumerRecord<Integer, Externalizable>> it = null;
            ConsumerRecord<Integer, Externalizable> next = null;
            boolean exhausted = false;

            @Override
            public boolean hasNext() {
                if (exhausted) return false;
                if (it == null) {
                    while (records == null || records.isEmpty()) {
                        records = consumer.poll(1000);
                        if (TaskContext.get().isInterrupted()) {
                            consumer.close();
                            throw new TaskKilledException();
                        }
                    }
                    it = records.iterator();
                }
                if (it.hasNext()) {
                    return true;
                }
                else {
                    consumer.close();
                    return false;
                }
            }

            @Override
            public ExecRow next() {
                return (ExecRow)it.next().value();
            }
        };
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(topicName);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        topicName = in.readUTF();
    }
}
