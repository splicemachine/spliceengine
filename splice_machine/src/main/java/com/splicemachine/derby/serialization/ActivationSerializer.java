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

package com.splicemachine.derby.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.spark_project.guava.collect.Lists;
import org.spark_project.guava.collect.Maps;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.derby.stream.ActivationHolder;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ParameterValueSet;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.execute.BaseActivation;
import com.splicemachine.db.impl.sql.execute.IndexRow;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;
import java.io.*;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

/**
 * entity for converting an Activation into a byte[] and back again
 *
 * @author Scott Fines
 * Created on: 5/17/13
 */
public class ActivationSerializer extends Serializer<ActivationHolder> {

    private static final Logger LOG = Logger.getLogger(ActivationSerializer.class);

    @Override
    public void write(Kryo kryo, Output output, ActivationHolder source) {
        if (classFactory == null) {
            LanguageConnectionContext lcc = source.getActivation().getLanguageConnectionContext();
            classFactory = lcc.getLanguageConnectionFactory().getClassFactory();
        }
        Visitor visitor = new Visitor(source);
        try {
            visitor.visitAll();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        visitor.write(kryo, output);
    }

    @Override
    public ActivationHolder read(Kryo kryo, Input input, Class type) {
        return null;  // this won't be used
    }

    public static Activation readInto(Kryo kryo, Input input,  ActivationHolder destination) {
        if (classFactory == null) {
            LanguageConnectionContext lcc = destination.getActivation().getLanguageConnectionContext();
            classFactory = lcc.getLanguageConnectionFactory().getClassFactory();
        }
        return new Visitor(destination).read(kryo, input);
    }

    private static final List<FieldStorageFactory> factories;
    private static final ArrayFactory arrayFactory;
    private static ClassFactory classFactory;
    private static Kryo kryo;

    static{
        factories = Lists.newArrayList();
        factories.add(new DataValueDescriptorFactory());
        factories.add(new ExecRowFactory());
        factories.add(new CachedOpFieldFactory());
        arrayFactory = new ArrayFactory();
        factories.add(arrayFactory);
        factories.add(new BooleanFactory());

        //always add SerializableFactory last, because otherwise it'll swallow everything else.
        factories.add(new SerializableFactory());
    }


    private static class Visitor{
        private final Map<Integer,SpliceOperation> operationsMap;
        private ActivationHolder activationHolder;
        private Map<String, FieldStorage> fields;
        private Map<String,FieldStorage> baseFields;
        private Activation activation;

        private Visitor(ActivationHolder activationHolder) {
            this.activationHolder = activationHolder;
            this.operationsMap = activationHolder.getOperationsMap();
            this.activation = activationHolder.getActivation();
            this.fields = Maps.newHashMap();
            this.baseFields = Maps.newHashMap();
        }

        void visitAll() throws IllegalAccessException, IOException {
            // first visit base fields, so we can reference ops from "resultSet" later on
            visitBaseActivationFields();
            visitSelfFields();
        }

        private void visitBaseActivationFields() throws IOException, IllegalAccessException {
            Class baseActClass = getBaseActClass(activation.getClass());
            if (baseActClass == null) return;

            try{
                visit(baseActClass.getDeclaredField("row"),baseFields);
                visit(baseActClass.getDeclaredField("resultSet"),baseFields);
                visit(baseActClass.getDeclaredField("materialized"),baseFields);
            } catch (NoSuchFieldException e) {
                SpliceLogUtils.warn(LOG, "Could not serialize current row list");
            }

        }


        private void visitSelfFields() throws IllegalAccessException {
            for(Field field:activation.getClass().getDeclaredFields()){
                visit(field,fields);
            }
        }

        void visit(Field field,Map<String,FieldStorage> storageMap) throws IllegalAccessException {
            //TODO -sf- cleaner way of removing fields we don't want to serialize?
            if(isQualifierType(field.getType())) return; //ignore qualifiers
            else if(ParameterValueSet.class.isAssignableFrom(field.getType())) return;
            else if(ResultDescription.class.isAssignableFrom(field.getType())) return;

            boolean isAccessible = field.isAccessible();
            if(!isAccessible)
                field.setAccessible(true);
            try{
                Object o = field.get(activation);
                if(o!=null){
                    FieldStorage storage = getFieldStorage(o, field.getType());
                    if(storage!=null)
                        storageMap.put(field.getName(),storage);
                }
            }finally{
                if(!isAccessible)
                    field.setAccessible(false);
            }
        }

        private void setField(Class clazz, String fieldName, FieldStorage storage){
            try {
                Field declaredField = clazz.getDeclaredField(fieldName);
                if(!declaredField.isAccessible()){
                    declaredField.setAccessible(true);
                    declaredField.set(activation,storage.getValue(activationHolder));
                    declaredField.setAccessible(false);
                }else
                    declaredField.set(activation,storage.getValue(activationHolder));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public void write(Kryo kryo, Output out) {
            out.writeInt(fields.size());
            for(Map.Entry<String,FieldStorage> entry:fields.entrySet()){
                String fieldName = entry.getKey();
                FieldStorage storage = entry.getValue();
                out.writeString(fieldName);
                kryo.writeClassAndObject(out, storage);
            }

            out.writeInt(baseFields.size());
            for(Map.Entry<String,FieldStorage> entry:baseFields.entrySet()){
                String fieldName = entry.getKey();
                FieldStorage storage = entry.getValue();
                out.writeString(fieldName);
                kryo.writeClassAndObject(out, storage);
            }
        }

        public Activation read(Kryo kryo, Input in) {
            int numFieldsToRead = in.readInt();
            Class actClass = activation.getClass();
            for(int i=0;i<numFieldsToRead;i++){
                String fieldName = in.readString();
                FieldStorage storage = (FieldStorage)kryo.readClassAndObject(in);
                setField(actClass, fieldName, storage);
            }

            int numBaseFieldsToRead = in.readInt();
            Class baseActClass = getBaseActClass(actClass);
            for(int i=0;i<numBaseFieldsToRead;i++){
                String fieldName = in.readString();
                FieldStorage storage = (FieldStorage)kryo.readClassAndObject(in);
                setField(baseActClass,fieldName,storage);
            }
            return activation;
        }
    }

    private static Class getBaseActClass(Class actClass) {
        Class baseActClass = actClass;
        while(baseActClass!=null &&!baseActClass.equals(BaseActivation.class))
            baseActClass = baseActClass.getSuperclass();
        if(baseActClass==null) return null;
        return baseActClass;
    }

    @SuppressWarnings("SimplifiableIfStatement")
    private static boolean isQualifierType(Class clazz) {
        while (true) {
            if (Qualifier.class.isAssignableFrom(clazz)) return true;
            else if (clazz.isArray()) {
                clazz = clazz.getComponentType();
            } else return false;
        }
    }

    private static FieldStorage getFieldStorage(Object o, Class<?> type) {
        for(FieldStorageFactory factory:factories){
            if(factory.isType(o, type)){
                return factory.create(o,type);
            }
        }
        return null;
    }

    private interface FieldStorage extends KryoSerializable {

        Object getValue(ActivationHolder context) throws StandardException;
    }

    private interface FieldStorageFactory<F extends FieldStorage> {
        F create(Object objectToStore, @SuppressWarnings("rawtypes") Class type);
        boolean isType(Object instance, Class type);
    }

    public static class BooleanFieldStorage implements FieldStorage{
        private Boolean data;

        public BooleanFieldStorage() { }

        public BooleanFieldStorage(Boolean value) {
            this.data = value;
        }

        @Override
        public void write (Kryo kryo, Output out) {
            out.writeBoolean(data);
        }

        @Override
        public void read (Kryo kryo, Input in) {
            this.data = in.readBoolean();
        }

        @Override
        public Object getValue(ActivationHolder context) throws StandardException {
            return data;
        }
    }

    private static class BooleanFactory implements FieldStorageFactory<BooleanFieldStorage> {
        @Override
        public BooleanFieldStorage create(Object objectToStore, Class type) {
            return new BooleanFieldStorage((Boolean)objectToStore);
        }

        @Override
        public boolean isType(Object instance, Class type) {
            return type.getName().compareToIgnoreCase("boolean")==0;
        }
    }

    public static class ArrayFieldStorage implements FieldStorage{
        private static final long serialVersionUID = 4l;
        private FieldStorage[] data;
        private Class arrayType;

        @Deprecated
        public ArrayFieldStorage() { }

        @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
        public ArrayFieldStorage(Class arrayType,FieldStorage[] fields) {
            this.data = fields;
            this.arrayType = arrayType;
        }

        @Override
        @SuppressWarnings("ForLoopReplaceableByForEach")
        public void write (Kryo kryo, Output out) {
            out.writeInt(data.length);
            for(int i=0;i<data.length;i++){
                FieldStorage storage = data[i];
                kryo.writeClassAndObject(out, storage);
            }
            kryo.writeClass(out, arrayType);
        }

        @Override
        public void read (Kryo kryo, Input in) {
            data = new FieldStorage[in.readInt()];
            for(int i=0;i<data.length;i++){
                data[i] = (FieldStorage)kryo.readClassAndObject(in);
            }
            arrayType = kryo.readClass(in).getType();
        }

        @Override
        public Object getValue(ActivationHolder context) throws StandardException {
            Object[] objects = (Object[]) Array.newInstance(arrayType, data.length);

            for(int i=0;i<objects.length;i++){
                FieldStorage storage = data[i];
                if(storage!=null)
                    objects[i] = storage.getValue(context);
            }
            return objects;
        }

        public int getSize() {
            return data.length;
        }
    }

    private static class ArrayFactory implements FieldStorageFactory<ArrayFieldStorage> {
        @Override
        public ArrayFieldStorage create(Object objectToStore, Class type) {
            FieldStorage[] fields = new FieldStorage[Array.getLength(objectToStore)];
            type = type.isArray()? type.getComponentType():type;
            for(int i=0;i<fields.length;i++){
                Object o = Array.get(objectToStore,i);
                if(o!=null){
                    fields[i] = getFieldStorage(o, type);
                }
            }
            return new ArrayFieldStorage(type,fields);
        }

        @Override
        public boolean isType(Object instance, Class type) {
            return type.isArray();
        }
    }


    private static class DataValueDescriptorFactory implements FieldStorageFactory<DataValueStorage>{

        @Override
        public DataValueStorage create(Object objectToStore, Class type) {
            DataValueDescriptor dvd = (DataValueDescriptor)objectToStore;
            return new DataValueStorage(dvd);
        }

        @Override
        public boolean isType(Object instance, Class type) {
            return DataValueDescriptor.class.isAssignableFrom(type);
        }
    }  

    public static class DataValueStorage implements FieldStorage {
        private static final long serialVersionUID = 1l;
        private DataValueDescriptor dvd;

        @Deprecated
        public DataValueStorage() { }

        public DataValueStorage(DataValueDescriptor dvd) {
            this.dvd = dvd;
        }

        @Override
        public Object getValue(ActivationHolder context) throws StandardException {
            return dvd;
        }

        @Override
        public void write (Kryo kryo, Output out) {
            kryo.writeClassAndObject(out, dvd);
        }

        @Override
        public void read (Kryo kryo, Input in) {
            dvd = (DataValueDescriptor) kryo.readClassAndObject(in);
        }
    }

    private static class ExecRowFactory implements FieldStorageFactory<ExecRowStorage>{

        @Override
        public ExecRowStorage create(Object objectToStore, Class type) {
            ExecRow row = (ExecRow)objectToStore;
            DataValueDescriptor[] dvds = row.getRowArray();
            ArrayFieldStorage fieldStorage = arrayFactory.create(dvds,DataValueDescriptor.class);
            return new ExecRowStorage(row instanceof IndexRow,fieldStorage);
        }

        @Override
        public boolean isType(Object instance, Class type) {
            return ExecRow.class.isAssignableFrom(type);
        }
    }

    public static class ExecRowStorage implements FieldStorage{
        private static final long serialVersionUID = 1l;
        private ArrayFieldStorage data;
        private boolean isIndexType;

        @Deprecated
        public ExecRowStorage() { }

        public ExecRowStorage(boolean isIndexType,ArrayFieldStorage data) {
            this.data = data;
            this.isIndexType = isIndexType;
        }

        @Override
        public Object getValue(ActivationHolder context) throws StandardException {

            ExecRow valueRow;

            DataValueDescriptor[] dvds = (DataValueDescriptor[])data.getValue(context);
            if(isIndexType){
                valueRow = context.getActivation().getExecutionFactory().getIndexableRow(dvds.length);
            }else
                valueRow = context.getActivation().getExecutionFactory().getValueRow(dvds.length);

            valueRow.setRowArray((DataValueDescriptor[])data.getValue(context));

            return valueRow;
        }

        @Override
        public void write (Kryo kryo, Output out) {
            out.writeBoolean(isIndexType);
            kryo.writeClassAndObject(out, data);
        }

        @Override
        public void read (Kryo kryo, Input in) {
            this.isIndexType = in.readBoolean();
            this.data = (ArrayFieldStorage)kryo.readClassAndObject(in);
        }
    }


    public static class CachedOpFieldFactory implements FieldStorageFactory<CachedOpFieldStorage> {

        @Override
        public CachedOpFieldStorage create(Object objectToStore, @SuppressWarnings("rawtypes") Class type) {
            return new CachedOpFieldStorage((SpliceOperation)objectToStore);
        }

        @Override
        public boolean isType(Object instance, Class type) {
            return instance instanceof SpliceOperation ;
        }
    }


    public static class CachedOpFieldStorage implements FieldStorage {
        private static final long serialVersionUID = 1l;
        private int resultSetNumber;

        private SpliceOperation operation;

        @Deprecated
        public CachedOpFieldStorage() {
        }

        public CachedOpFieldStorage(SpliceOperation operation) {
            this.operation = operation;
            this.resultSetNumber = operation.resultSetNumber();
        }

        @Override
        public Object getValue(ActivationHolder context) throws StandardException {
            return context.getOperationsMap().get(resultSetNumber);
        }

        @Override
        public void write (Kryo kryo, Output out) {
            out.writeInt(resultSetNumber);
        }

        @Override
        public void read (Kryo kryo, Input in) {
            resultSetNumber = in.readInt();
        }
    }


    private static class SerializableFactory implements FieldStorageFactory<SerializableStorage>{

        @Override
        public SerializableStorage create(Object objectToStore, Class type) {
            return new SerializableStorage((Serializable)objectToStore);
        }

        @Override
        public boolean isType(Object instance, Class type) {
            return Serializable.class.isAssignableFrom(type);
        }
    }
    public static class SerializableStorage implements FieldStorage{
        private static final long serialVersionUID = 1l;
        private Serializable data;

        @Deprecated
        public SerializableStorage() { }

        public SerializableStorage(Serializable data) {
            this.data = data;
        }

        @Override
        public Object getValue(ActivationHolder context) throws StandardException {
            return data;
        }

        @Override
        public void write (Kryo kryo, Output out) {
            kryo.writeClassAndObject(out, data);
        }

        @Override
        public void read (Kryo kryo, Input in) {
            data = (Serializable)kryo.readClassAndObject(in);
        }
    }
}
