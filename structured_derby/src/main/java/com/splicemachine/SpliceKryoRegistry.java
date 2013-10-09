package com.splicemachine;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.derby.hbase.ActivationSerializer;
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
    //ExternalizableSerializers are stateless, no need to create more than we need
    private static final ExternalizableSerializer EXTERNALIZABLE_SERIALIZER = new ExternalizableSerializer();
    @Override
    public void register(Kryo instance) {
        instance.setReferences(false);
        instance.register(GenericStorablePreparedStatement.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(DataTypeDescriptor.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(TypeDescriptorImpl.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(BaseTypeIdImpl.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(UserDefinedTypeIdImpl.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(DecimalTypeIdImpl.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(RowMultiSetImpl.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(RoutineAliasInfo.class,EXTERNALIZABLE_SERIALIZER);

        instance.register(IndexConglomerate.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(HBaseConglomerate.class,EXTERNALIZABLE_SERIALIZER);

        instance.register(FormatableBitSet.class,EXTERNALIZABLE_SERIALIZER);

        instance.register(CountAggregator.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SumAggregator.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(AvgAggregator.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(MaxMinAggregator.class,EXTERNALIZABLE_SERIALIZER);

        instance.register(SQLDecimal.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SQLDouble.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SQLInteger.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SQLVarchar.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SQLChar.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SQLBoolean.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SQLBit.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SQLDate.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SQLTime.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SQLTimestamp.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SQLSmallint.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SQLTinyint.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SQLLongint.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SQLLongvarchar.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SQLReal.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SQLRef.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(LazyDataValueDescriptor.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(LazyNumberDataValueDescriptor.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(LazyStringDataValueDescriptor.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(TaskStatus.class,EXTERNALIZABLE_SERIALIZER);


        //register Activation-related classes
        instance.register(ActivationSerializer.ArrayFieldStorage.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(ActivationSerializer.DataValueStorage.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(ActivationSerializer.ExecRowStorage.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(ActivationSerializer.SerializableStorage.class,EXTERNALIZABLE_SERIALIZER);

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
