package com.splicemachine;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.derby.hbase.ActivationSerializer;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.LazyDataValueDescriptor;
import com.splicemachine.derby.impl.sql.execute.LazyNumberDataValueDescriptor;
import com.splicemachine.derby.impl.sql.execute.LazyStringDataValueDescriptor;
import com.splicemachine.derby.impl.sql.execute.actions.DeleteConstantOperation;
import com.splicemachine.derby.impl.sql.execute.actions.InsertConstantOperation;
import com.splicemachine.derby.impl.sql.execute.actions.UpdateConstantOperation;
import com.splicemachine.derby.impl.sql.execute.operations.*;
import com.splicemachine.derby.impl.store.access.btree.IndexConglomerate;
import com.splicemachine.derby.impl.store.access.hbase.HBaseConglomerate;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.stats.TimingStats;
import com.splicemachine.derby.utils.kryo.DataValueDescriptorSerializer;
import com.splicemachine.derby.utils.kryo.ValueRowSerializer;
import com.splicemachine.hbase.writer.BulkWrite;
import com.splicemachine.hbase.writer.KVPair;
import com.splicemachine.job.ErrorTransport;
import com.splicemachine.job.TaskStatus;
import com.splicemachine.utils.kryo.ExternalizableSerializer;
import com.splicemachine.utils.kryo.KryoPool;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.TreeMap;

import org.apache.derby.catalog.types.BaseTypeIdImpl;
import org.apache.derby.catalog.types.DecimalTypeIdImpl;
import org.apache.derby.catalog.types.DefaultInfoImpl;
import org.apache.derby.catalog.types.IndexDescriptorImpl;
import org.apache.derby.catalog.types.ReferencedColumnsDescriptorImpl;
import org.apache.derby.catalog.types.RoutineAliasInfo;
import org.apache.derby.catalog.types.RowMultiSetImpl;
import org.apache.derby.catalog.types.SynonymAliasInfo;
import org.apache.derby.catalog.types.TypeDescriptorImpl;
import org.apache.derby.catalog.types.UserDefinedTypeIdImpl;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableArrayHolder;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.FormatableHashtable;
import org.apache.derby.iapi.services.io.FormatableInstanceGetter;
import org.apache.derby.iapi.services.io.FormatableIntHolder;
import org.apache.derby.iapi.services.io.FormatableLongHolder;
import org.apache.derby.iapi.sql.dictionary.IndexRowGenerator;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.store.raw.ContainerKey;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.SQLBit;
import org.apache.derby.iapi.types.SQLBlob;
import org.apache.derby.iapi.types.SQLBoolean;
import org.apache.derby.iapi.types.SQLChar;
import org.apache.derby.iapi.types.SQLClob;
import org.apache.derby.iapi.types.SQLDate;
import org.apache.derby.iapi.types.SQLDecimal;
import org.apache.derby.iapi.types.SQLDouble;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.derby.iapi.types.SQLLongVarbit;
import org.apache.derby.iapi.types.SQLLongint;
import org.apache.derby.iapi.types.SQLLongvarchar;
import org.apache.derby.iapi.types.SQLReal;
import org.apache.derby.iapi.types.SQLRef;
import org.apache.derby.iapi.types.SQLSmallint;
import org.apache.derby.iapi.types.SQLTime;
import org.apache.derby.iapi.types.SQLTimestamp;
import org.apache.derby.iapi.types.SQLTinyint;
import org.apache.derby.iapi.types.SQLVarbit;
import org.apache.derby.iapi.types.SQLVarchar;
import org.apache.derby.iapi.types.UserType;
import org.apache.derby.impl.services.uuid.BasicUUID;
import org.apache.derby.impl.sql.CursorInfo;
import org.apache.derby.impl.sql.CursorTableReference;
import org.apache.derby.impl.sql.GenericColumnDescriptor;
import org.apache.derby.impl.sql.GenericParameter;
import org.apache.derby.impl.sql.GenericParameterValueSet;
import org.apache.derby.impl.sql.GenericResultDescription;
import org.apache.derby.impl.sql.GenericStorablePreparedStatement;
import org.apache.derby.impl.sql.catalog.DDColumnDependableFinder;
import org.apache.derby.impl.sql.catalog.DD_Version;
import org.apache.derby.impl.sql.catalog.DDdependableFinder;
import org.apache.derby.impl.sql.execute.AggregatorInfo;
import org.apache.derby.impl.sql.execute.AggregatorInfoList;
import org.apache.derby.impl.sql.execute.AvgAggregator;
import org.apache.derby.impl.sql.execute.CountAggregator;
import org.apache.derby.impl.sql.execute.FKInfo;
import org.apache.derby.impl.sql.execute.IndexColumnOrder;
import org.apache.derby.impl.sql.execute.MaxMinAggregator;
import org.apache.derby.impl.sql.execute.SumAggregator;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.apache.derby.impl.store.access.PC_XenaVersion;

/**
 * 
 * TODO Utilize unsafe with Kryo 2.2
 * https://github.com/EsotericSoftware/kryo
 * 
 * @author Scott Fines
 * Created on: 8/15/13
 */
public class SpliceKryoRegistry implements KryoPool.KryoRegistry{
    //ExternalizableSerializers are stateless, no need to create more than we need
    private static final ExternalizableSerializer EXTERNALIZABLE_SERIALIZER = new ExternalizableSerializer();
    @Override
    public void register(Kryo instance) {
    	instance.setReferences(false);
        instance.setRegistrationRequired(true);

        instance.register(ValueRow.class,new ValueRowSerializer());
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

        instance.register(SQLDecimal.class,new DataValueDescriptorSerializer<SQLDecimal>(){
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLDecimal object) throws StandardException {
                kryo.writeObjectOrNull(output, object.getObject(), BigDecimal.class);
            }
            @Override
            protected void readValue(Kryo kryo, Input input, SQLDecimal dvd) throws StandardException {
                dvd.setBigDecimal(kryo.readObjectOrNull(input, BigDecimal.class));
            }
        });
        instance.register(SQLDouble.class,new DataValueDescriptorSerializer<SQLDouble>(){
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLDouble object) throws StandardException {
                output.writeDouble(object.getDouble());
            }
            @Override
            protected void readValue(Kryo kryo, Input input, SQLDouble dvd) throws StandardException {
                dvd.setValue(input.readDouble());
            }
        });
        instance.register(SQLInteger.class,new DataValueDescriptorSerializer<SQLInteger>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLInteger object) {
                output.writeInt(object.getInt());
            }
            @Override
            protected void readValue(Kryo kryo, Input input, SQLInteger dvd) {
                dvd.setValue(input.readInt());
            }
        });
        instance.register(SQLVarchar.class,new DataValueDescriptorSerializer<SQLVarchar>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLVarchar object) throws StandardException {
                output.writeString(object.getString());
            }
            @Override
            protected void readValue(Kryo kryo, Input input, SQLVarchar dvd) {
                dvd.setValue(input.readString());
            }
        });
        instance.register(SQLLongvarchar.class,new DataValueDescriptorSerializer<SQLLongvarchar>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLLongvarchar object) throws StandardException {
                output.writeString(object.getString());
            }
            @Override
            protected void readValue(Kryo kryo, Input input, SQLLongvarchar dvd) {
                dvd.setValue(input.readString());
            }
        });
        instance.register(SQLChar.class,new DataValueDescriptorSerializer<SQLChar>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLChar object) throws StandardException {
                output.writeString(object.getString());
            }
            @Override
            protected void readValue(Kryo kryo, Input input, SQLChar dvd) {
                dvd.setValue(input.readString());
            }
        });
        instance.register(SQLBoolean.class,new DataValueDescriptorSerializer<SQLBoolean>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLBoolean object) throws StandardException {
                output.writeBoolean(object.getBoolean());
            }

            @Override
            protected void readValue(Kryo kryo, Input input, SQLBoolean dvd) throws StandardException {
                dvd.setValue(input.readBoolean());
            }
        });
        instance.register(SQLBit.class,new DataValueDescriptorSerializer<SQLBit>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLBit object) throws StandardException {
                byte[] data = object.getBytes();
                output.writeInt(data.length);
                output.write(data);
            }

            @Override
            protected void readValue(Kryo kryo, Input input, SQLBit dvd) throws StandardException {
                byte[] data = new byte[input.readInt()];
                //noinspection ResultOfMethodCallIgnored
                input.read(data);
                dvd.setValue(data);
            }
        });
        instance.register(SQLVarbit.class,new DataValueDescriptorSerializer<SQLVarbit>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLVarbit object) throws StandardException {
                byte[] data = object.getBytes();
                output.writeInt(data.length);
                output.write(data);
            }

            @Override
            protected void readValue(Kryo kryo, Input input, SQLVarbit dvd) throws StandardException {
                byte[] data = new byte[input.readInt()];
                //noinspection ResultOfMethodCallIgnored
                input.read(data);
                dvd.setValue(data);
            }
        });        
        instance.register(SQLLongVarbit.class,new DataValueDescriptorSerializer<SQLLongVarbit>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLLongVarbit object) throws StandardException {
                byte[] data = object.getBytes();
                output.writeInt(data.length);
                output.write(data);
            }

            @Override
            protected void readValue(Kryo kryo, Input input, SQLLongVarbit dvd) throws StandardException {
                byte[] data = new byte[input.readInt()];
                //noinspection ResultOfMethodCallIgnored
                input.read(data);
                dvd.setValue(data);
            }
        });        

        instance.register(SQLDate.class,new DataValueDescriptorSerializer<SQLDate>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLDate object) throws StandardException {
                output.writeLong(object.getDate((Calendar) null).getTime());
            }

            @Override
            protected void readValue(Kryo kryo, Input input, SQLDate dvd) throws StandardException {
                dvd.setValue(new Date(input.readLong()));
            }
        });
        instance.register(SQLTime.class,new DataValueDescriptorSerializer<SQLTime>(){
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLTime object) throws StandardException {
                output.writeLong(object.getTime(null).getTime());
            }

            @Override
            protected void readValue(Kryo kryo, Input input, SQLTime dvd) throws StandardException {
                dvd.setValue(new Time(input.readLong()));
            }
        });
        instance.register(SQLTimestamp.class,new DataValueDescriptorSerializer<SQLTimestamp>(){
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLTimestamp object) throws StandardException {
                output.writeLong(object.getTimestamp(null).getTime());
            }

            @Override
            protected void readValue(Kryo kryo, Input input, SQLTimestamp dvd) throws StandardException {
                dvd.setValue(new Timestamp(input.readLong()));
            }
        });
        instance.register(SQLSmallint.class,new DataValueDescriptorSerializer<SQLSmallint>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLSmallint object) throws StandardException {
                output.writeShort(object.getShort());
            }
            @Override
            protected void readValue(Kryo kryo, Input input, SQLSmallint dvd) throws StandardException {
                dvd.setValue(input.readShort());
            }
        });
        instance.register(SQLTinyint.class,new DataValueDescriptorSerializer<SQLTinyint>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLTinyint object) throws StandardException {
                output.writeByte(object.getByte());
            }
            @Override
            protected void readValue(Kryo kryo, Input input, SQLTinyint dvd) throws StandardException {
                dvd.setValue(input.readByte());
            }
        });

        instance.register(SQLLongint.class,new DataValueDescriptorSerializer<SQLLongint>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLLongint object) throws StandardException {
                output.writeLong(object.getLong());
            }
            @Override
            protected void readValue(Kryo kryo, Input input, SQLLongint dvd) throws StandardException {
                dvd.setValue(input.readLong());
            }
        });
        instance.register(SQLReal.class, new DataValueDescriptorSerializer<SQLReal>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLReal object) throws StandardException {
                output.writeFloat(object.getFloat());
            }

            @Override
            protected void readValue(Kryo kryo, Input input, SQLReal dvd) throws StandardException {
                dvd.setValue(input.readFloat());
            }
        });
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

        instance.register(SpliceObserverInstructions.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SpliceObserverInstructions.ActivationContext.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(GenericParameterValueSet.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(GenericParameter.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SchemaDescriptor.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SpliceRuntimeContext.class,EXTERNALIZABLE_SERIALIZER);

        instance.register(ProjectRestrictOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(TableScanOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(BulkTableScanOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(GroupedAggregateOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(DistinctScanOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(DistinctScalarAggregateOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(IndexRowToBaseRowOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(SortOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(UnionOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(UpdateOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(UpdateConstantOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(InsertOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(DeleteOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(MergeSortJoinOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(NestedLoopJoinOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(NestedLoopLeftOuterJoinOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(MergeSortLeftOuterJoinOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(ScalarAggregateOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(NormalizeOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(AnyOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(RowOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(OnceOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(RowCountOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(BroadcastJoinOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(BroadcastLeftOuterJoinOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(DerbyOperationInformation.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(DerbyScanInformation.class,EXTERNALIZABLE_SERIALIZER);

        instance.register(PC_XenaVersion.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(BasicUUID.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(IndexDescriptorImpl.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(FormatableHashtable.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(FormatableIntHolder.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(FormatableIntHolder[].class);
        instance.register(DD_Version.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(DDdependableFinder.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(DDColumnDependableFinder.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(byte[].class);
        instance.register(boolean[].class);
        instance.register(int[].class);
        instance.register(double[].class);
        instance.register(float[].class);
        instance.register(long[].class);
        instance.register(Collections.emptyList().getClass());
        instance.register(CursorInfo.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(GenericResultDescription.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(GenericColumnDescriptor.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(ReferencedColumnsDescriptorImpl.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(ErrorTransport.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(DefaultInfoImpl.class,EXTERNALIZABLE_SERIALIZER);

        instance.register(BigDecimal.class);
        instance.register(HashMap.class);
        instance.register(TreeMap.class);
        instance.register(ArrayList.class);
        instance.register(LinkedList.class);

        instance.register(FormatableArrayHolder.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(IndexColumnOrder.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(HBaseRowLocation.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(TaskStats.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(TimingStats.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(AggregatorInfoList.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(AggregatorInfo.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(UserType.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(InsertConstantOperation.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(DerbyDMLWriteInfo.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(DeleteConstantOperation.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(FormatableLongHolder.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(FormatableInstanceGetter.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(SpliceRuntimeContext.Path.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(IndexRowGenerator.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(MultiProbeDerbyScanInformation.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(MultiProbeTableScanOperation.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(DistinctGroupedAggregateOperation.class,EXTERNALIZABLE_SERIALIZER);



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

        instance.register(SQLClob.class,new DataValueDescriptorSerializer<SQLClob>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLClob object) throws StandardException {
                output.writeString(object.getString());
            }
            @Override
            protected void readValue(Kryo kryo, Input input, SQLClob dvd) {
                dvd.setValue(input.readString());
            }
        });
        instance.register(SQLBlob.class,new DataValueDescriptorSerializer<SQLBlob>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLBlob object) throws StandardException {
                byte[] data = object.getBytes();
                output.writeInt(data.length);
                output.write(data);
            }

            @Override
            protected void readValue(Kryo kryo, Input input, SQLBlob dvd) throws StandardException {
                byte[] data = new byte[input.readInt()];
                input.read(data);
                dvd.setValue(data);
            }
        });
        instance.register(SynonymAliasInfo.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(CursorTableReference.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(FKInfo.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(DerbyAggregateContext.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(DerbyGroupedAggregateContext.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(LastIndexKeyOperation.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(MergeJoinOperation.class, EXTERNALIZABLE_SERIALIZER);
        instance.register(BulkWrite.class,EXTERNALIZABLE_SERIALIZER);
        instance.register(KVPair.class,EXTERNALIZABLE_SERIALIZER);
    }
}
