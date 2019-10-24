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

package com.splicemachine;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import com.esotericsoftware.kryo.serializers.MapSerializer;
import com.splicemachine.db.catalog.types.*;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.*;
import com.splicemachine.db.iapi.sql.dictionary.IndexRowGenerator;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TriggerDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.stats.ColumnStatisticsImpl;
import com.splicemachine.db.iapi.stats.ColumnStatisticsMerge;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.impl.services.uuid.BasicUUID;
import com.splicemachine.db.impl.sql.*;
import com.splicemachine.db.impl.sql.catalog.DDColumnDependableFinder;
import com.splicemachine.db.impl.sql.catalog.DD_Version;
import com.splicemachine.db.impl.sql.catalog.DDdependableFinder;
import com.splicemachine.db.impl.sql.catalog.ManagedCache;
import com.splicemachine.db.impl.sql.execute.*;
import com.splicemachine.db.impl.store.access.PC_XenaVersion;
import com.splicemachine.db.shared.common.udt.UDTBase;
import com.splicemachine.derby.ddl.DDLChangeType;
import com.splicemachine.derby.ddl.TentativeAddColumnDesc;
import com.splicemachine.derby.ddl.TentativeAddConstraintDesc;
import com.splicemachine.derby.ddl.TentativeDropColumnDesc;
import com.splicemachine.derby.impl.sql.catalog.Splice_DD_Version;
import com.splicemachine.derby.impl.sql.execute.actions.DeleteConstantOperation;
import com.splicemachine.derby.impl.sql.execute.actions.InsertConstantOperation;
import com.splicemachine.derby.impl.sql.execute.actions.UpdateConstantOperation;
import com.splicemachine.derby.impl.sql.execute.operations.*;
import com.splicemachine.derby.impl.sql.execute.operations.batchonce.BatchOnceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.export.ExportFile;
import com.splicemachine.derby.impl.sql.execute.operations.export.ExportOperation;
import com.splicemachine.derby.impl.sql.execute.operations.export.ExportParams;
import com.splicemachine.derby.impl.sql.execute.operations.framework.DerbyAggregateContext;
import com.splicemachine.derby.impl.sql.execute.operations.groupedaggregate.DerbyGroupedAggregateContext;
import com.splicemachine.derby.impl.sql.execute.operations.window.DerbyWindowContext;
import com.splicemachine.derby.impl.store.access.btree.IndexConglomerate;
import com.splicemachine.derby.impl.store.access.hbase.HBaseConglomerate;
import com.splicemachine.derby.serialization.ActivationSerializer;
import com.splicemachine.derby.serialization.SpliceObserverInstructions;
import com.splicemachine.derby.stream.ActivationHolder;
import com.splicemachine.derby.stream.control.BadRecordsRecorder;
import com.splicemachine.derby.stream.control.ControlOperationContext;
import com.splicemachine.derby.stream.function.*;
import com.splicemachine.derby.utils.kryo.DataValueDescriptorSerializer;
import com.splicemachine.derby.utils.kryo.ListDataTypeSerializer;
import com.splicemachine.derby.utils.kryo.SimpleObjectSerializer;
import com.splicemachine.derby.utils.kryo.ValueRowSerializer;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.client.BulkWrite;
import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.pipeline.constraint.ConstraintContext;
import com.splicemachine.si.impl.txn.ActiveWriteTxn;
import com.splicemachine.si.impl.txn.WritableTxn;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.kryo.ExternalizableSerializer;
import com.splicemachine.utils.kryo.KryoObjectInput;
import com.splicemachine.utils.kryo.KryoObjectOutput;
import com.splicemachine.utils.kryo.KryoPool;
import de.javakaffee.kryoserializers.ArraysAsListSerializer;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.spark_project.guava.base.Optional;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Properties;
import java.util.TreeMap;


/**
 * Maps serializable entities to a Kryo Serializer, so that
 * they may be more efficiently serialized than raw Java serialization.
 * <p/>
 * https://github.com/EsotericSoftware/kryo
 *
 * @author Scott Fines
 *         Created on: 8/15/13
 */
public class SpliceKryoRegistry implements KryoPool.KryoRegistry{
    //ExternalizableSerializers are stateless, no need to create more than we need
    private static final ExternalizableSerializer EXTERNALIZABLE_SERIALIZER=ExternalizableSerializer.INSTANCE;
    private static final UnmodifiableCollectionsSerializer UNMODIFIABLE_COLLECTIONS_SERIALIZER=new UnmodifiableCollectionsSerializer();
    private static final DefaultSerializers.KryoSerializableSerializer KRYO_SERIALIZABLE_SERIALIZER = new DefaultSerializers.KryoSerializableSerializer();

    private static volatile KryoPool spliceKryoPool;

    public static Class<?> getClassFromString(String className) {
        Class<?> foundClass = null;
        try {
            foundClass = Class.forName(className);
        } catch ( final Exception e ) {
            throw new RuntimeException( e );
        }
        return foundClass;
    }

    public static KryoPool getInstance(){
        KryoPool kp = spliceKryoPool;
        if(kp==null){
            synchronized(SpliceKryoRegistry.class){
                kp = spliceKryoPool;
                if(kp==null){
                    EngineDriver driver = EngineDriver.driver();
                    if(driver==null){
                        kp = new KryoPool(1);
                    }else{
                        int kpSize = driver.getConfiguration().getKryoPoolSize();
                        kp = spliceKryoPool = new KryoPool(kpSize);
                    }
                    kp.setKryoRegistry(new SpliceKryoRegistry());
                }
            }
        }
        return kp;
    }

    @Override
    public void register(Kryo instance){
                /*
				 * IMPORTANT!!!! READ THIS COMMENT!!!
				 * ===========================================================
			   * If you don't read this comment fully and COMPLETELY
			   * understand what's going on with this registry, then you run
			   * the risk of doing something which breaks Splice on existing
			   * clusters; when you do that, Customer Solutions and QA will
			   * complain to Gene, who will in turn complain to Scott, who
			   * will in turn hunt you down and stab you with a razor-sharp
			   * shard of glass for not paying attention to this message. You
			   * Have Been Warned.
			   * ==========================================================
			   *
			   * When Kryo serializes an object, the first thing it will
			   * write is an integer identifier. This identifier UNIQUELY
			   * and UNEQUIVOCABLY represents the entirety of the class
			   * information for the serialized object.
			   *
			   * In most cases, this isn't much of a problem, but we use
			   * Kryo to serialize objects into tables (in particular,
			   * into System tables). What this means is that if the
			   * identifier for a class that's stored in a table, then
			   * we can NEVER change the identifier FOR ANY REASON.
			   *
			   * So, in practice, what does that mean for you? Well,
			   * each line of the following block of code will assign
			   * a unique, manually-defined id to a class. This allows
			   * you to rearrange lines (which wasn't always true), but
			   * means that you MUST follow these rules when adding to this
			   * class
			   *
			   * 1. do NOT refactor away the magic numbers--if you try
			   * and be clever with a counter, you WILL break backwards compatibility
			   *
			   * 2. Once the rest of the world has seen a line with a specific Id,
			   * NEVER CHANGE THAT ID. In practice, once you've pushed a change to this
			   * file, that change can NEVER BE UNDONE. So make sure it's right.
			   *
			   * To register a new class, here's what you do.
			   *
			   * 1. create a serializer class (or reuse one)
			   * 2. find the HIGHEST PREVIOUSLY REGISTERED id.
			   * 3. register your new class with the NEXT id in the monotonic sequence
			   * (e.g. if the highest is 145, register with 146).
			   * 4. run the ENTIRE IT suite WITHOUT cleaning your data (an easy protection
			   * against accidentally breaking system tables)
			   * 5. warn everyone that you've made changes to this file (in particular, warn
			   * Scott and Gene, so that they can be aware of your potentially breaking
			   * backwards compatibility).
			   * 6. push ONLY AFTER YOU ARE SURE that it works exactly as intended.
			   *
			   * To change how a class is being serialized:
			   *
			   * 1. DON'T. You'll probably break backwards compatibility.
			   *
			   * If you absolutely MUST change the serialization, then you'll need
			   * to ensure somehow that your change is backwards compatible. To do this:
			   *
			   * 1. confirm that your class isn't stored anywhere. If it's never written
			   *  to disk, then you are safe to make changes. Most classes that are known
			   *  to never be written to disk should have a comment in this file about
			   *  it being safe to change
			   * 2. If the file IS stored EVER, then you'll need to devise a way to convert
			   * existing stored instances into the new format. Usually, that means you'll need
			   * to be able to read either format (but only need to write in the new),
			   * which will make your deserialization code complicated and difficult,
			   * but that's the nature of the beast.
	    	   */
        instance.setReferences(false);
        instance.setRegistrationRequired(true);

        instance.register(ValueRow.class,new ValueRowSerializer<ValueRow>(){
            @Override
            protected ValueRow newType(int size){
                return new ValueRow(size);
            }
        },10);
        instance.register(GenericStorablePreparedStatement.class,EXTERNALIZABLE_SERIALIZER,11);
        instance.register(DataTypeDescriptor.class,EXTERNALIZABLE_SERIALIZER,18);
        instance.register(TypeDescriptorImpl.class,EXTERNALIZABLE_SERIALIZER,12);
        instance.register(BaseTypeIdImpl.class,EXTERNALIZABLE_SERIALIZER,13);
        instance.register(UserDefinedTypeIdImpl.class,EXTERNALIZABLE_SERIALIZER,14);
        instance.register(DecimalTypeIdImpl.class,EXTERNALIZABLE_SERIALIZER,15);
        instance.register(RowMultiSetImpl.class,EXTERNALIZABLE_SERIALIZER,16);
        instance.register(RoutineAliasInfo.class,EXTERNALIZABLE_SERIALIZER,17);

        instance.register(IndexConglomerate.class,EXTERNALIZABLE_SERIALIZER,19);
        instance.register(HBaseConglomerate.class,EXTERNALIZABLE_SERIALIZER,20);

        instance.register(FormatableBitSet.class,EXTERNALIZABLE_SERIALIZER,21);

        instance.register(CountAggregator.class,EXTERNALIZABLE_SERIALIZER,22);
        instance.register(SumAggregator.class,EXTERNALIZABLE_SERIALIZER,23);
        instance.register(AvgAggregator.class,EXTERNALIZABLE_SERIALIZER,24);
        instance.register(MaxMinAggregator.class,EXTERNALIZABLE_SERIALIZER,25);

        instance.register(SQLDecimal.class,new DataValueDescriptorSerializer<SQLDecimal>(){
            @Override
            protected void writeValue(Kryo kryo,Output output,SQLDecimal object) throws StandardException{
                kryo.writeObjectOrNull(output,object.getObject(),BigDecimal.class);
            }

            @Override
            protected void readValue(Kryo kryo,Input input,SQLDecimal dvd) throws StandardException{
                dvd.setBigDecimal(kryo.readObjectOrNull(input,BigDecimal.class));
            }
        },26);
        instance.register(SQLDouble.class,new DataValueDescriptorSerializer<SQLDouble>(){
            @Override
            protected void writeValue(Kryo kryo,Output output,SQLDouble object) throws StandardException{
                output.writeDouble(object.getDouble());
            }

            @Override
            protected void readValue(Kryo kryo,Input input,SQLDouble dvd) throws StandardException{
                dvd.setValue(input.readDouble());
            }
        },27);
        instance.register(SQLInteger.class,new DataValueDescriptorSerializer<SQLInteger>(){
            @Override
            protected void writeValue(Kryo kryo,Output output,SQLInteger object){
                output.writeInt(object.getInt());
            }

            @Override
            protected void readValue(Kryo kryo,Input input,SQLInteger dvd){
                dvd.setValue(input.readInt());
            }
        },28);
        instance.register(SQLVarchar.class,new DataValueDescriptorSerializer<SQLVarchar>(){
            @Override
            protected void writeValue(Kryo kryo,Output output,SQLVarchar object) throws StandardException{
                output.writeString(object.getString());
            }

            @Override
            protected void readValue(Kryo kryo,Input input,SQLVarchar dvd){
                dvd.setValue(input.readString());
            }
        },29);
        instance.register(SQLLongvarchar.class,new DataValueDescriptorSerializer<SQLLongvarchar>(){
            @Override
            protected void writeValue(Kryo kryo,Output output,SQLLongvarchar object) throws StandardException{
                output.writeString(object.getString());
            }

            @Override
            protected void readValue(Kryo kryo,Input input,SQLLongvarchar dvd){
                dvd.setValue(input.readString());
            }
        },30);
        instance.register(SQLChar.class,new DataValueDescriptorSerializer<SQLChar>(){
            @Override
            protected void writeValue(Kryo kryo,Output output,SQLChar object) throws StandardException{
                output.writeString(object.getString());
            }

            @Override
            protected void readValue(Kryo kryo,Input input,SQLChar dvd){
                dvd.setValue(input.readString());
            }
        },31);
        instance.register(SQLBoolean.class,new DataValueDescriptorSerializer<SQLBoolean>(){
            @Override
            protected void writeValue(Kryo kryo,Output output,SQLBoolean object) throws StandardException{
                output.writeBoolean(object.getBoolean());
            }

            @Override
            protected void readValue(Kryo kryo,Input input,SQLBoolean dvd) throws StandardException{
                dvd.setValue(input.readBoolean());
            }
        },32);
        instance.register(SQLBit.class,new DataValueDescriptorSerializer<SQLBit>(){
            @Override
            protected void writeValue(Kryo kryo,Output output,SQLBit object) throws StandardException{
                byte[] data=object.getBytes();
                output.writeInt(data.length);
                output.write(data);
            }

            @Override
            protected void readValue(Kryo kryo,Input input,SQLBit dvd) throws StandardException{
                int size=input.readInt();
                byte[] data=new byte[size];
                //noinspection ResultOfMethodCallIgnored
                int bytesRead=input.read(data);
                assert bytesRead == size: "Did not read entire data point!";
                dvd.setValue(data);
            }
        },33);
        instance.register(SQLVarbit.class,new DataValueDescriptorSerializer<SQLVarbit>(){
            @Override
            protected void writeValue(Kryo kryo,Output output,SQLVarbit object) throws StandardException{
                byte[] data=object.getBytes();
                output.writeInt(data.length);
                output.write(data);
            }

            @Override
            protected void readValue(Kryo kryo,Input input,SQLVarbit dvd) throws StandardException{
                int size=input.readInt();
                byte[] data=new byte[size];
                int read=input.read(data);
                assert read== size: "Did not read entire data";
                dvd.setValue(data);
            }
        },34);
        instance.register(SQLLongVarbit.class,new DataValueDescriptorSerializer<SQLLongVarbit>(){
            @Override
            protected void writeValue(Kryo kryo,Output output,SQLLongVarbit object) throws StandardException{
                byte[] data=object.getBytes();
                output.writeInt(data.length);
                output.write(data);
            }

            @Override
            protected void readValue(Kryo kryo,Input input,SQLLongVarbit dvd) throws StandardException{
                int size=input.readInt();
                byte[] data=new byte[size];
                int read=input.read(data);
                assert read==size: "Did not read entire data";
                dvd.setValue(data);
            }
        },35);

        instance.register(SQLDate.class,new DataValueDescriptorSerializer<SQLDate>(){
            @Override
            protected void writeValue(Kryo kryo,Output output,SQLDate object) throws StandardException{
                output.writeLong(object.getDate((Calendar)null).getTime());
            }

            @Override
            protected void readValue(Kryo kryo,Input input,SQLDate dvd) throws StandardException{
                dvd.setValue(new Date(input.readLong()));
            }
        },36);
        instance.register(SQLTime.class,new DataValueDescriptorSerializer<SQLTime>(){
            @Override
            protected void writeValue(Kryo kryo,Output output,SQLTime object) throws StandardException{
                output.writeLong(object.getTime(null).getTime());
            }

            @Override
            protected void readValue(Kryo kryo,Input input,SQLTime dvd) throws StandardException{
                dvd.setValue(new Time(input.readLong()));
            }
        },37);
        instance.register(SQLTimestamp.class,new DataValueDescriptorSerializer<SQLTimestamp>(){
            @Override
            protected void writeValue(Kryo kryo,Output output,SQLTimestamp object) throws StandardException{
                output.writeLong(object.getTimestamp(null).getTime());
            }

            @Override
            protected void readValue(Kryo kryo,Input input,SQLTimestamp dvd) throws StandardException{
                dvd.setValue(new Timestamp(input.readLong()));
            }
        },38);
        instance.register(SQLSmallint.class,new DataValueDescriptorSerializer<SQLSmallint>(){
            @Override
            protected void writeValue(Kryo kryo,Output output,SQLSmallint object) throws StandardException{
                output.writeShort(object.getShort());
            }

            @Override
            protected void readValue(Kryo kryo,Input input,SQLSmallint dvd) throws StandardException{
                dvd.setValue(input.readShort());
            }
        },39);
        instance.register(SQLTinyint.class,new DataValueDescriptorSerializer<SQLTinyint>(){
            @Override
            protected void writeValue(Kryo kryo,Output output,SQLTinyint object) throws StandardException{
                output.writeByte(object.getByte());
            }

            @Override
            protected void readValue(Kryo kryo,Input input,SQLTinyint dvd) throws StandardException{
                dvd.setValue(input.readByte());
            }
        },40);

        instance.register(SQLLongint.class,new DataValueDescriptorSerializer<SQLLongint>(){
            @Override
            protected void writeValue(Kryo kryo,Output output,SQLLongint object) throws StandardException{
                output.writeLong(object.getLong());
            }

            @Override
            protected void readValue(Kryo kryo,Input input,SQLLongint dvd) throws StandardException{
                dvd.setValue(input.readLong());
            }
        },41);
        instance.register(SQLReal.class,new DataValueDescriptorSerializer<SQLReal>(){
            @Override
            protected void writeValue(Kryo kryo,Output output,SQLReal object) throws StandardException{
                output.writeFloat(object.getFloat());
            }

            @Override
            protected void readValue(Kryo kryo,Input input,SQLReal dvd) throws StandardException{
                dvd.setValue(input.readFloat());
            }
        },42);
        // Note that serialization of SQLRef as part of an ExecRow is handled by RefDescriptorSerializer
        instance.register(SQLRef.class,EXTERNALIZABLE_SERIALIZER,43);
        instance.register(ListDataType.class, new ListDataTypeSerializer<ListDataType>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, ListDataType object) throws StandardException {
                int forLim = object.getLength();
                output.writeInt(forLim);
                for (int i = 0; i < forLim; i++) {
                    boolean dvdPresent = (object.getDVD(i) != null);
                    output.writeBoolean(dvdPresent);
                    if (dvdPresent)
                        kryo.writeClassAndObject(output, object.getDVD(i));
                }
            }
        
            @Override
            protected void readValue(Kryo kryo, Input input, ListDataType dvd) {
                int forLim = input.readInt();
                dvd.setLength(forLim);
                for (int i=0; i < forLim; i++) {
                    DataValueDescriptor inputDVD;
                    if (input.readBoolean()) {
                        inputDVD = (DataValueDescriptor) kryo.readClassAndObject(input);
                        try {
                            dvd.setFrom(inputDVD, i);
                        } catch (StandardException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        }, 44);
        instance.register(org.apache.commons.lang3.mutable.MutableDouble.class,
                          new SimpleObjectSerializer<MutableDouble>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, MutableDouble object) throws RuntimeException {
                output.writeDouble(object.doubleValue());
            }

            @Override
            protected void readValue(Kryo kryo, Input input, MutableDouble object) throws RuntimeException {
                object.setValue(input.readDouble());
            }
        }, 45);
        /*instance.register(SQLRef.class, new DataValueDescriptorSerializer<SQLRef>() {
            @Override
            protected void writeValue(Kryo kryo, Output output, SQLRef object) throws StandardException {
                kryo.writeObjectOrNull(output, object.getObject(), HBaseRowLocation.class);
            }

            @Override
            protected void readValue(Kryo kryo, Input input, SQLRef dvd) throws StandardException {
                dvd.setValue((RowLocation)(kryo.readObjectOrNull(input, HBaseRowLocation.class)));
            }
        },43);*/


        //register Activation-related classes
        instance.register(ActivationSerializer.ArrayFieldStorage.class,KRYO_SERIALIZABLE_SERIALIZER,48);
        instance.register(ActivationSerializer.DataValueStorage.class,KRYO_SERIALIZABLE_SERIALIZER,49);
        instance.register(ActivationSerializer.ExecRowStorage.class,KRYO_SERIALIZABLE_SERIALIZER,50);
        instance.register(ActivationSerializer.SerializableStorage.class,KRYO_SERIALIZABLE_SERIALIZER,51);
        instance.register(ActivationSerializer.CachedOpFieldStorage.class,KRYO_SERIALIZABLE_SERIALIZER,151);

        instance.register(SpliceObserverInstructions.class,EXTERNALIZABLE_SERIALIZER,52);
        instance.register(SpliceObserverInstructions.ActivationContext.class,EXTERNALIZABLE_SERIALIZER,53);
        instance.register(GenericParameterValueSet.class,EXTERNALIZABLE_SERIALIZER,54);
        instance.register(GenericParameter.class,EXTERNALIZABLE_SERIALIZER,55);
        instance.register(SchemaDescriptor.class,EXTERNALIZABLE_SERIALIZER,56);
        instance.register(ProjectRestrictOperation.class,EXTERNALIZABLE_SERIALIZER,58);
        instance.register(TableScanOperation.class,EXTERNALIZABLE_SERIALIZER,59);
        instance.register(ScanOperation.class,EXTERNALIZABLE_SERIALIZER,60);
        instance.register(GroupedAggregateOperation.class,EXTERNALIZABLE_SERIALIZER,61);
        instance.register(DistinctScanOperation.class,EXTERNALIZABLE_SERIALIZER,62);
        instance.register(DistinctScalarAggregateOperation.class,EXTERNALIZABLE_SERIALIZER,63);
        instance.register(IndexRowToBaseRowOperation.class,EXTERNALIZABLE_SERIALIZER,64);
        instance.register(SortOperation.class,EXTERNALIZABLE_SERIALIZER,65);
        instance.register(UnionOperation.class,EXTERNALIZABLE_SERIALIZER,66);
        instance.register(UpdateOperation.class,EXTERNALIZABLE_SERIALIZER,67);
        instance.register(UpdateConstantOperation.class,EXTERNALIZABLE_SERIALIZER,68);
        instance.register(InsertOperation.class,EXTERNALIZABLE_SERIALIZER,69);
        instance.register(DeleteOperation.class,EXTERNALIZABLE_SERIALIZER,70);
        instance.register(MergeSortJoinOperation.class,EXTERNALIZABLE_SERIALIZER,71);
        instance.register(NestedLoopJoinOperation.class,EXTERNALIZABLE_SERIALIZER,72);
        instance.register(NestedLoopLeftOuterJoinOperation.class,EXTERNALIZABLE_SERIALIZER,73);
        instance.register(MergeSortLeftOuterJoinOperation.class,EXTERNALIZABLE_SERIALIZER,74);
        instance.register(ScalarAggregateOperation.class,EXTERNALIZABLE_SERIALIZER,75);
        instance.register(NormalizeOperation.class,EXTERNALIZABLE_SERIALIZER,76);
        instance.register(AnyOperation.class,EXTERNALIZABLE_SERIALIZER,77);
        instance.register(RowOperation.class,EXTERNALIZABLE_SERIALIZER,78);
        instance.register(OnceOperation.class,EXTERNALIZABLE_SERIALIZER,79);
        instance.register(RowCountOperation.class,EXTERNALIZABLE_SERIALIZER,80);
        instance.register(BroadcastJoinOperation.class,EXTERNALIZABLE_SERIALIZER,81);
        instance.register(BroadcastLeftOuterJoinOperation.class,EXTERNALIZABLE_SERIALIZER,82);
        instance.register(DerbyOperationInformation.class,EXTERNALIZABLE_SERIALIZER,83);
        instance.register(DerbyScanInformation.class,EXTERNALIZABLE_SERIALIZER,89);
        instance.register(CachedOperation.class,EXTERNALIZABLE_SERIALIZER,150);

        instance.register(PC_XenaVersion.class,EXTERNALIZABLE_SERIALIZER,84);
        instance.register(BasicUUID.class,EXTERNALIZABLE_SERIALIZER,85);
        instance.register(IndexDescriptorImpl.class,EXTERNALIZABLE_SERIALIZER,86);
        instance.register(FormatableHashtable.class,EXTERNALIZABLE_SERIALIZER,87);
        instance.register(FormatableIntHolder.class,EXTERNALIZABLE_SERIALIZER,88);
        instance.register(FormatableIntHolder[].class,91);
        instance.register(DD_Version.class,EXTERNALIZABLE_SERIALIZER,90);
        instance.register(DDdependableFinder.class,EXTERNALIZABLE_SERIALIZER,92);
        instance.register(DDColumnDependableFinder.class,EXTERNALIZABLE_SERIALIZER,93);
        instance.register(byte[].class,94);
        instance.register(boolean[].class,95);
        instance.register(int[].class,96);
        instance.register(double[].class,97);
        instance.register(float[].class,98);
        instance.register(long[].class,99);
        instance.register(Collections.emptyList().getClass(),100);
        instance.register(Collections.unmodifiableList(new LinkedList()).getClass(),UNMODIFIABLE_COLLECTIONS_SERIALIZER,152);
        instance.register(Collections.unmodifiableList(new ArrayList()).getClass(),UNMODIFIABLE_COLLECTIONS_SERIALIZER,153);
        instance.register(CursorInfo.class,EXTERNALIZABLE_SERIALIZER,101);
        instance.register(GenericResultDescription.class,EXTERNALIZABLE_SERIALIZER,102);
        instance.register(GenericColumnDescriptor.class,EXTERNALIZABLE_SERIALIZER,103);
        instance.register(ReferencedColumnsDescriptorImpl.class,EXTERNALIZABLE_SERIALIZER,104);
//        instance.register(ErrorTransport.class,EXTERNALIZABLE_SERIALIZER,105);
        instance.register(DefaultInfoImpl.class,EXTERNALIZABLE_SERIALIZER,106);

        instance.register(BigDecimal.class,107);
        instance.register(HashMap.class,108);
        instance.register(TreeMap.class,109);
        instance.register(ArrayList.class,110);
        instance.register(LinkedList.class,111);

        instance.register(FormatableArrayHolder.class,EXTERNALIZABLE_SERIALIZER,112);
        instance.register(IndexColumnOrder.class,EXTERNALIZABLE_SERIALIZER,113);
        instance.register(HBaseRowLocation.class,EXTERNALIZABLE_SERIALIZER,114);
        instance.register(AggregatorInfoList.class,EXTERNALIZABLE_SERIALIZER,117);
        instance.register(AggregatorInfo.class,EXTERNALIZABLE_SERIALIZER,118);
        instance.register(UserType.class,EXTERNALIZABLE_SERIALIZER,119);
        instance.register(InsertConstantOperation.class,EXTERNALIZABLE_SERIALIZER,120);
        instance.register(DerbyDMLWriteInfo.class,EXTERNALIZABLE_SERIALIZER,121);
        instance.register(DeleteConstantOperation.class,EXTERNALIZABLE_SERIALIZER,122);
        instance.register(FormatableLongHolder.class,EXTERNALIZABLE_SERIALIZER,123);
        instance.register(FormatableInstanceGetter.class,EXTERNALIZABLE_SERIALIZER,124);
        instance.register(IndexRowGenerator.class,EXTERNALIZABLE_SERIALIZER,126);
        instance.register(MultiProbeDerbyScanInformation.class,EXTERNALIZABLE_SERIALIZER,127);
        instance.register(MultiProbeTableScanOperation.class,EXTERNALIZABLE_SERIALIZER,128);
        instance.register(DistinctGroupedAggregateOperation.class,EXTERNALIZABLE_SERIALIZER,129);
        instance.register(SQLClob.class,new DataValueDescriptorSerializer<SQLClob>(){
            @Override
            protected void writeValue(Kryo kryo,Output output,SQLClob object) throws StandardException{
                output.writeString(object.getString());
            }

            @Override
            protected void readValue(Kryo kryo,Input input,SQLClob dvd){
                dvd.setValue(input.readString());
            }
        },131);
        instance.register(SQLBlob.class,new DataValueDescriptorSerializer<SQLBlob>(){
            @Override
            protected void writeValue(Kryo kryo,Output output,SQLBlob object) throws StandardException{
                byte[] data=object.getBytes();
                output.writeInt(data.length);
                output.write(data);
            }

            @Override
            protected void readValue(Kryo kryo,Input input,SQLBlob dvd) throws StandardException{
                int size=input.readInt();
                byte[] data=new byte[size];
                int read = input.read(data);
                assert read==size: "Did not read entire data line!";
                dvd.setValue(data);
            }
        },132);
        instance.register(SynonymAliasInfo.class,EXTERNALIZABLE_SERIALIZER,133);
        instance.register(CursorTableReference.class,EXTERNALIZABLE_SERIALIZER,134);
        instance.register(FKInfo.class,EXTERNALIZABLE_SERIALIZER,135);
        instance.register(DerbyAggregateContext.class,EXTERNALIZABLE_SERIALIZER,136);
        instance.register(DerbyGroupedAggregateContext.class,EXTERNALIZABLE_SERIALIZER,137);
        instance.register(LastIndexKeyOperation.class,EXTERNALIZABLE_SERIALIZER,138);
        instance.register(MergeJoinOperation.class,EXTERNALIZABLE_SERIALIZER,139);

        instance.register(AggregateAliasInfo.class,EXTERNALIZABLE_SERIALIZER,140);
        instance.register(UserDefinedAggregator.class,EXTERNALIZABLE_SERIALIZER,141);
        instance.register(BulkWrite.class,new Serializer<BulkWrite>(){
            @Override
            public void write(Kryo kryo,Output output,BulkWrite object){
                output.writeByte(object.getFlags());
                output.writeString(object.getEncodedStringName());
                kryo.writeClassAndObject(output,object.getMutations());
            }

            @Override
            public BulkWrite read(Kryo kryo,Input input,Class type){
                byte skipWrite=input.readByte();
                String eSn=input.readString();
                Collection<KVPair> kvPairs=(Collection<KVPair>)kryo.readClassAndObject(input);
                return new BulkWrite(kvPairs,eSn,skipWrite);
            }
        },142);
        instance.register(KVPair.class,new Serializer<KVPair>(){
            @Override
            public void write(Kryo kryo,Output output,KVPair object){
                output.writeByte(object.getType().asByte());
                byte[] rowKey=object.getRowKey();
                output.writeInt(rowKey.length);
                output.writeBytes(rowKey);
                byte[] value=object.getValue();
                output.writeInt(value.length);
                output.writeBytes(value);
            }

            @Override
            public KVPair read(Kryo kryo,Input input,Class type){
                KVPair.Type t=KVPair.Type.decode(input.readByte());
                byte[] rowKey=new byte[input.readInt()];
                input.readBytes(rowKey);
                byte[] value=new byte[input.readInt()];
                input.readBytes(value);
                return new KVPair(rowKey,value,t);
            }
        },143);
        instance.register(SpliceStddevPop.class,EXTERNALIZABLE_SERIALIZER,144);
        instance.register(SpliceStddevSamp.class,EXTERNALIZABLE_SERIALIZER,145);
        instance.register(Properties.class,new MapSerializer(),146);

        //instance.register(com.splicemachine.derby.client.sql.execute.ValueRow.class,EXTERNALIZABLE_SERIALIZER,147);
        instance.register(com.splicemachine.db.impl.sql.execute.IndexRow.class,
                new ValueRowSerializer<com.splicemachine.db.impl.sql.execute.IndexRow>(){
                    @Override
                    public void write(Kryo kryo,Output output,com.splicemachine.db.impl.sql.execute.IndexRow object){
                        super.write(kryo,output,object);
                        boolean[] orderedNulls=object.getOrderedNulls();
                        output.writeInt(orderedNulls.length);
                        for(boolean orderedNull : orderedNulls){
                            output.writeBoolean(orderedNull);
                        }
                    }

                    @Override
                    public com.splicemachine.db.impl.sql.execute.IndexRow read(Kryo kryo,
                                                                               Input input,
                                                                               Class<com.splicemachine.db.impl.sql.execute.IndexRow> type){
                        com.splicemachine.db.impl.sql.execute.IndexRow row=super.read(kryo,input,type);
                        boolean[] orderedNulls=new boolean[input.readInt()];
                        for(int i=0;i<orderedNulls.length;i++){
                            orderedNulls[i]=input.readBoolean();
                        }
                        row.setOrderedNulls(orderedNulls);
                        return row;
                    }

                    @Override
                    protected com.splicemachine.db.impl.sql.execute.IndexRow newType(int size){
                        return com.splicemachine.db.impl.sql.execute.IndexRow.createRaw(size);
                    }
                },149);

//        instance.register(ActivationSerializer.OperationResultSetStorage.class,EXTERNALIZABLE_SERIALIZER,155);
        instance.register(ByteSlice.class,EXTERNALIZABLE_SERIALIZER,156);
        instance.register(LongBufferedSumAggregator.class,new Serializer<LongBufferedSumAggregator>(){
            @Override
            public void write(Kryo kryo,Output output,LongBufferedSumAggregator object){
                try{
                    object.writeExternal(new KryoObjectOutput(output,kryo));
                }catch(IOException e){
                    throw new RuntimeException(e);
                }
            }

            @Override
            public LongBufferedSumAggregator read(Kryo kryo,Input input,Class type){
                LongBufferedSumAggregator aggregator=new LongBufferedSumAggregator(64); //todo -sf- make configurable
                try{
                    aggregator.readExternal(new KryoObjectInput(input,kryo));
                }catch(IOException|ClassNotFoundException e){
                    throw new RuntimeException(e);
                }
                return aggregator;
            }
        },158);

        instance.register(DoubleBufferedSumAggregator.class,new Serializer<DoubleBufferedSumAggregator>(){
            @Override
            public void write(Kryo kryo,Output output,DoubleBufferedSumAggregator object){
                try{
                    object.writeExternal(new KryoObjectOutput(output,kryo));
                }catch(IOException e){
                    throw new RuntimeException(e);
                }
            }

            @Override
            public DoubleBufferedSumAggregator read(Kryo kryo,Input input,Class type){
                DoubleBufferedSumAggregator aggregator=new DoubleBufferedSumAggregator(64); //todo -sf- make configurable
                try{
                    aggregator.readExternal(new KryoObjectInput(input,kryo));
                }catch(IOException|ClassNotFoundException e){
                    throw new RuntimeException(e);
                }
                return aggregator;
            }
        },159);

        instance.register(DecimalBufferedSumAggregator.class,new Serializer<DecimalBufferedSumAggregator>(){
            @Override
            public void write(Kryo kryo,Output output,DecimalBufferedSumAggregator object){
                try{
                    object.writeExternal(new KryoObjectOutput(output,kryo));
                }catch(IOException e){
                    throw new RuntimeException(e);
                }
            }

            @Override
            public DecimalBufferedSumAggregator read(Kryo kryo,Input input,Class type){
                DecimalBufferedSumAggregator aggregator=new DecimalBufferedSumAggregator(64); //todo -sf- make configurable
                try{
                    aggregator.readExternal(new KryoObjectInput(input,kryo));
                }catch(IOException|ClassNotFoundException e){
                    throw new RuntimeException(e);
                }
                return aggregator;
            }
        },160);

        instance.register(WindowOperation.class,EXTERNALIZABLE_SERIALIZER,179);
        // instance.register(HashNestedLoopJoinOperation.class,EXTERNALIZABLE_SERIALIZER,180);
        // instance.register(HashNestedLoopLeftOuterJoinOperation.class,EXTERNALIZABLE_SERIALIZER,181);
        instance.register(MergeLeftOuterJoinOperation.class,EXTERNALIZABLE_SERIALIZER,182);
        instance.register(DerbyWindowContext.class,EXTERNALIZABLE_SERIALIZER,184);
        instance.register(ActivationSerializer.BooleanFieldStorage.class, KRYO_SERIALIZABLE_SERIALIZER,185);
        instance.register(WindowFunctionInfoList.class,EXTERNALIZABLE_SERIALIZER,186);
        instance.register(WindowFunctionInfo.class,EXTERNALIZABLE_SERIALIZER,187);
        instance.register(TentativeDropColumnDesc.class,EXTERNALIZABLE_SERIALIZER,190);
        instance.register(DerbyWindowContext.class,EXTERNALIZABLE_SERIALIZER,191);
        instance.register(ActivationSerializer.BooleanFieldStorage.class,KRYO_SERIALIZABLE_SERIALIZER,192);
        instance.register(ColumnInfo.class,EXTERNALIZABLE_SERIALIZER,193);
        instance.register(DDLChangeType.class,195);
        instance.register(ExportOperation.class,EXTERNALIZABLE_SERIALIZER,197);
        instance.register(ExportParams.class,198);
        instance.register(SQLRowId.class,EXTERNALIZABLE_SERIALIZER,202);
        instance.register(Splice_DD_Version.class,EXTERNALIZABLE_SERIALIZER,203);
//        instance.register(BulkWriteResult.class, BulkWriteResult.kryoSerializer(),204);
//        instance.register(BulkWrites.class, new Serializer<BulkWrites>() {
//            @Override
//            public void write(Kryo kryo, Output output, BulkWrites object) {
//                byte[] txnBytes = TransactionOperations.getOperationFactory().encode(object.getTxn());
//                output.writeInt(txnBytes.length);
//                output.write(txnBytes);
//                kryo.writeClassAndObject(output,object.getBulkWrites());
//            }
//
//            @Override
//            public BulkWrites read(Kryo kryo, Input input, Class type) {
//                byte[] txnBytes = input.readBytes(input.readInt());
//                TxnView txn = TransactionOperations.getOperationFactory().decode(txnBytes,0,txnBytes.length);
//                Collection<BulkWrite> bws = (Collection<BulkWrite>)kryo.readClassAndObject(input);
//                return new BulkWrites(bws,txn);
//            }
//        }, 206);
        instance.register(ActiveWriteTxn.class,EXTERNALIZABLE_SERIALIZER,207);
        instance.register(WriteResult.class,EXTERNALIZABLE_SERIALIZER,209);
        instance.register(ConstraintContext.class,EXTERNALIZABLE_SERIALIZER,210);
        instance.register(WritableTxn.class,EXTERNALIZABLE_SERIALIZER,211);
        // instance.register(SinkTask.class, EXTERNALIZABLE_SERIALIZER,212);
        // 220 is open
        // 221 is open
//        instance.register(CreateBackupTask.class, EXTERNALIZABLE_SERIALIZER,222);
//        instance.register(RestoreBackupTask.class, EXTERNALIZABLE_SERIALIZER,223);
//        instance.register(PurgeTransactionsTask.class, EXTERNALIZABLE_SERIALIZER,224);
//        instance.register(BackupItem.class, EXTERNALIZABLE_SERIALIZER,225);
//        instance.register(Backup.class, EXTERNALIZABLE_SERIALIZER,226);
//        instance.register(BackupItem.RegionInfo.class, EXTERNALIZABLE_SERIALIZER,227);
//        instance.register(Pair.class, 228);
//        instance.register(CreateIncrementalBackupTask.class, EXTERNALIZABLE_SERIALIZER,230);
        instance.register(TentativeAddConstraintDesc.class,EXTERNALIZABLE_SERIALIZER,232);
        instance.register(TentativeAddColumnDesc.class,EXTERNALIZABLE_SERIALIZER,233);

        //statistics

        instance.register(TriggerInfo.class,EXTERNALIZABLE_SERIALIZER,253);
        instance.register(TriggerDescriptor.class,EXTERNALIZABLE_SERIALIZER,254);
        instance.register(GenericPreparedStatement.class,EXTERNALIZABLE_SERIALIZER,255);
//        instance.register(TentativeDropPKConstraintDesc.class,EXTERNALIZABLE_SERIALIZER,256);
        instance.register(TriggerExecutionStack.class,EXTERNALIZABLE_SERIALIZER,257);
        instance.register(TriggerExecutionContext.class,EXTERNALIZABLE_SERIALIZER,258);
        instance.register(BatchOnceOperation.class,EXTERNALIZABLE_SERIALIZER,261);
        instance.register(ScrollInsensitiveOperation.class,EXTERNALIZABLE_SERIALIZER,263);
        instance.register(VTIOperation.class,EXTERNALIZABLE_SERIALIZER,264);
        instance.register(UDTAliasInfo.class,EXTERNALIZABLE_SERIALIZER,271);
        instance.register(UDTBase.class,EXTERNALIZABLE_SERIALIZER,272);
        instance.register(HalfMergeSortJoinOperation.class,EXTERNALIZABLE_SERIALIZER,274);
        instance.register(HalfMergeSortLeftOuterJoinOperation.class,EXTERNALIZABLE_SERIALIZER,275);
        instance.register(ColumnStatisticsImpl.class,EXTERNALIZABLE_SERIALIZER,276);
        instance.register(StatisticsFlatMapFunction.class,EXTERNALIZABLE_SERIALIZER,277);
        instance.register(ColumnStatisticsMerge.class,EXTERNALIZABLE_SERIALIZER,278);
        instance.register(RowToLocatedRowFunction.class,EXTERNALIZABLE_SERIALIZER,279);

        instance.register(SQLArray.class,EXTERNALIZABLE_SERIALIZER,280);
        instance.register(ExternalizableFlatMapFunction.class,EXTERNALIZABLE_SERIALIZER,281);
        instance.register(SpliceBaseOperation.class,EXTERNALIZABLE_SERIALIZER,283);
        instance.register(CallStatementOperation.class,EXTERNALIZABLE_SERIALIZER,284);

        instance.register(MergeNonDistinctAggregatesFunctionForMixedRows.class,EXTERNALIZABLE_SERIALIZER, 285);
        instance.register(MergeAllAggregatesFunctionForMixedRows.class,EXTERNALIZABLE_SERIALIZER, 286);
        instance.register(DistinctAggregatesPrepareFunction.class,EXTERNALIZABLE_SERIALIZER, 287);
        instance.register(StitchMixedRowFunction.class,EXTERNALIZABLE_SERIALIZER, 288);
        instance.register(StitchMixedRowFlatMapFunction.class,EXTERNALIZABLE_SERIALIZER, 289);
        instance.register(DistinctAggregateKeyCreation.class,EXTERNALIZABLE_SERIALIZER, 290);
        instance.register(MergeStatisticsHolder.class,EXTERNALIZABLE_SERIALIZER,291);
        instance.register(ColumnStatisticsMerge.class,EXTERNALIZABLE_SERIALIZER,292);
        instance.register(SpliceUDAVariance.class,EXTERNALIZABLE_SERIALIZER,293);

        instance.register(SpliceBaseOperation.class,EXTERNALIZABLE_SERIALIZER,294);
        instance.register(ControlOperationContext.class,EXTERNALIZABLE_SERIALIZER,295);
        instance.register(ActivationHolder.class,EXTERNALIZABLE_SERIALIZER,296);
        instance.register(SetOpOperation.class,EXTERNALIZABLE_SERIALIZER,297);
        instance.register(ExportFile.COMPRESSION.class, 298);
        instance.register(FormatableProperties.class,EXTERNALIZABLE_SERIALIZER, 299);
        instance.register(BadRecordsRecorder.class,EXTERNALIZABLE_SERIALIZER, 300);
        instance.register(SelfReferenceOperation.class,new Serializer<SelfReferenceOperation>(){
            @Override
            public void write(Kryo kryo,Output output,SelfReferenceOperation object){
                try{
                    object.writeExternalWithoutChild(new KryoObjectOutput(output,kryo));
                }catch(IOException e){
                    throw new RuntimeException(e);
                }
            }

            @Override
            public SelfReferenceOperation read(Kryo kryo,Input input,Class type){
                SelfReferenceOperation selfReferenceOperation=new SelfReferenceOperation();
                try{
                    selfReferenceOperation.readExternal(new KryoObjectInput(input,kryo));
                }catch(IOException|ClassNotFoundException e){
                    throw new RuntimeException(e);
                }
                return selfReferenceOperation;
            }
        },301);
        instance.register(RecursiveUnionOperation.class,new Serializer<RecursiveUnionOperation>(){
            @Override
            public void write(Kryo kryo,Output output,RecursiveUnionOperation object){
                try{
                    object.writeExternal(new KryoObjectOutput(output,kryo));
                }catch(IOException e){
                    throw new RuntimeException(e);
                }
            }

            @Override
            public RecursiveUnionOperation read(Kryo kryo,Input input,Class type){
                RecursiveUnionOperation recursiveUnionOperation=new RecursiveUnionOperation();
                try{
                    recursiveUnionOperation.readExternal(new KryoObjectInput(input,kryo));
                    recursiveUnionOperation.setRecursiveUnionReference(recursiveUnionOperation);
                }catch(IOException|ClassNotFoundException e){
                    throw new RuntimeException(e);
                }
                return recursiveUnionOperation;
            }
        },302);
        instance.register(CrossJoinOperation.class, EXTERNALIZABLE_SERIALIZER, 303);
        instance.register(ManagedCache.class, EXTERNALIZABLE_SERIALIZER, 304);

        Serializer<Optional> optionalSerializer = new Serializer<Optional>() {
            @Override
            public void write(Kryo kryo, Output output, Optional object) {
                if (object.isPresent()) {
                    output.writeBoolean(false);
                    kryo.writeClassAndObject(output, object.get());
                } else {
                    output.writeBoolean(true);
                }
            }

            @Override
            public Optional read(Kryo kryo, Input input, Class<Optional> type) {
                boolean isNull = input.readBoolean();
                if (isNull) {
                    return Optional.absent();
                } else {
                    return Optional.of(kryo.readClassAndObject(input));
                }
            }
        };

        instance.register(Optional.class, optionalSerializer, 305);
        instance.register(Optional.absent().getClass(), optionalSerializer, 306);
        instance.register(Optional.of("").getClass(), optionalSerializer, 307);
        instance.register(ExecRow.class, 308);
        instance.register(ResultSet.class, 309);
        instance.register(ResultSet[].class, 310);
        instance.register(DataValueDescriptor.class, 311);
        instance.register(getClassFromString("java.util.Arrays$ArrayList"),
                          new ArraysAsListSerializer(), 312);
    }
}
