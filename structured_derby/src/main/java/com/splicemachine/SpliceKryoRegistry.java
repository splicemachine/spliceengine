package com.splicemachine;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.derby.impl.sql.execute.LazyDataValueDescriptor;
import com.splicemachine.derby.impl.sql.execute.LazyNumberDataValueDescriptor;
import com.splicemachine.derby.impl.sql.execute.LazyStringDataValueDescriptor;
import com.splicemachine.derby.impl.store.access.btree.IndexConglomerate;
import com.splicemachine.derby.impl.store.access.hbase.HBaseConglomerate;
import com.splicemachine.job.TaskStatus;
import com.splicemachine.utils.kryo.ExternalizableSerializer;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.derby.catalog.types.*;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.store.raw.ContainerKey;
import org.apache.derby.iapi.types.*;
import org.apache.derby.impl.sql.GenericStorablePreparedStatement;
import org.apache.derby.impl.sql.execute.AvgAggregator;
import org.apache.derby.impl.sql.execute.CountAggregator;
import org.apache.derby.impl.sql.execute.MaxMinAggregator;
import org.apache.derby.impl.sql.execute.SumAggregator;

/**
 * @author Scott Fines
 * Created on: 8/15/13
 */
public class SpliceKryoRegistry implements KryoPool.KryoRegistry{
    @Override
    public void register(Kryo instance) {
        instance.setReferences(false);
        instance.register(GenericStorablePreparedStatement.class,new ExternalizableSerializer());
        instance.register(DataTypeDescriptor.class,new ExternalizableSerializer());
        instance.register(TypeDescriptorImpl.class,new ExternalizableSerializer());
        instance.register(BaseTypeIdImpl.class,new ExternalizableSerializer());
        instance.register(UserDefinedTypeIdImpl.class,new ExternalizableSerializer());
        instance.register(DecimalTypeIdImpl.class,new ExternalizableSerializer());
        instance.register(RowMultiSetImpl.class,new ExternalizableSerializer());
        instance.register(RoutineAliasInfo.class,new ExternalizableSerializer());

        instance.register(IndexConglomerate.class,new ExternalizableSerializer());
        instance.register(HBaseConglomerate.class,new ExternalizableSerializer());

        instance.register(FormatableBitSet.class,new ExternalizableSerializer());

        instance.register(CountAggregator.class,new ExternalizableSerializer());
        instance.register(SumAggregator.class,new ExternalizableSerializer());
        instance.register(AvgAggregator.class,new ExternalizableSerializer());
        instance.register(MaxMinAggregator.class,new ExternalizableSerializer());

        instance.register(SQLDecimal.class,new ExternalizableSerializer());
        instance.register(SQLDouble.class,new ExternalizableSerializer());
        instance.register(SQLInteger.class,new ExternalizableSerializer());
        instance.register(SQLVarchar.class,new ExternalizableSerializer());
        instance.register(SQLChar.class,new ExternalizableSerializer());
        instance.register(SQLBoolean.class,new ExternalizableSerializer());
        instance.register(SQLBit.class,new ExternalizableSerializer());
        instance.register(SQLDate.class,new ExternalizableSerializer());
        instance.register(SQLTime.class,new ExternalizableSerializer());
        instance.register(SQLTimestamp.class,new ExternalizableSerializer());
        instance.register(SQLSmallint.class,new ExternalizableSerializer());
        instance.register(SQLTinyint.class,new ExternalizableSerializer());
        instance.register(SQLLongint.class,new ExternalizableSerializer());
        instance.register(SQLLongvarchar.class,new ExternalizableSerializer());
        instance.register(SQLReal.class,new ExternalizableSerializer());
        instance.register(SQLRef.class,new ExternalizableSerializer());
        instance.register(LazyDataValueDescriptor.class,new ExternalizableSerializer());
        instance.register(LazyNumberDataValueDescriptor.class,new ExternalizableSerializer());
        instance.register(LazyStringDataValueDescriptor.class,new ExternalizableSerializer());
        instance.register(TaskStatus.class,new ExternalizableSerializer());


        instance.register(ContainerKey.class,new Serializer<ContainerKey>() {
            @Override
            public void write(Kryo kryo, Output output, ContainerKey object) {
                output.writeLong(object.getContainerId());
                output.writeLong(object.getSegmentId());
            }

            @Override
            public ContainerKey read(Kryo kryo, Input input, Class<ContainerKey> type) {
                long containerId = input.readLong();
                long segmentId = input.readLong();
                return new ContainerKey(segmentId,containerId);
            }
        });
    }
}
