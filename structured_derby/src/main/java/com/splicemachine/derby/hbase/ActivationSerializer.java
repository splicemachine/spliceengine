package com.splicemachine.derby.hbase;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.ArrayUtil;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ParameterValueSet;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.impl.sql.execute.BaseActivation;
import org.apache.derby.impl.sql.execute.IndexRow;
import org.apache.log4j.Logger;

import java.io.*;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

/**
 * entity for converting an Activation into a byte[] and back again
 *
 * @author Scott Fines
 * Created on: 5/17/13
 */
@SuppressWarnings("UnusedDeclaration")
public class ActivationSerializer {

    private static final Logger LOG = Logger.getLogger(ActivationSerializer.class);

    public static void write(Activation source,ObjectOutput out) throws IOException {
        Visitor visitor = new Visitor(source);
        try {
            visitor.visitAll();
        } catch (IllegalAccessException e) {
            throw new IOException(e);
        }
        visitor.write(out);
    }

    public static Activation readInto(ObjectInput in, Activation destination) throws IOException, StandardException {
        try {
            return new Visitor(destination).read(in);
        } catch (IllegalAccessException e) {
            throw new IOException(e);
        } catch (NoSuchFieldException e) {
            throw new IOException(e);
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

    private static final List<FieldStorageFactory> factories;
    private static final ArrayFactory arrayFactory;

    static{
        factories = Lists.newArrayList();
        factories.add(new DataValueDescriptorFactory());
        factories.add(new ExecRowFactory());

        arrayFactory = new ArrayFactory();
        factories.add(arrayFactory);

        //always add SerializableFactory last, because otherwise it'll swallow everything else.
        factories.add(new SerializableFactory());
    }

    private static class Visitor{
        private Map<String,FieldStorage> fields;
        private Map<String,FieldStorage> baseFields;
        private Activation activation;

        private Visitor(Activation activation) {
            this.activation = activation;
            this.fields = Maps.newHashMap();
            this.baseFields = Maps.newHashMap();
        }

        void visitAll() throws IllegalAccessException, IOException {
            visitSelfFields();

            visitBaseActivationFields();

        }

        private void visitBaseActivationFields() throws IOException, IllegalAccessException {
            Class baseActClass = getBaseActClass(activation.getClass());
            if (baseActClass == null) return;

            try{
                visit(baseActClass.getDeclaredField("row"),baseFields);
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
            else if(ResultSet.class.isAssignableFrom(field.getType())) return; //serialize operations elsewhere
            else if(ParameterValueSet.class.isAssignableFrom(field.getType())) return;
            else if(ResultDescription.class.isAssignableFrom(field.getType())) return;

            boolean isAccessible = field.isAccessible();
            if(!isAccessible)
                field.setAccessible(true);
            try{
                Object o = field.get(activation);
                if(o!=null){
                    FieldStorage storage = getFieldStorage(o,field.getType());
                    if(storage!=null)
                        storageMap.put(field.getName(),storage);
                }
            }finally{
                if(!isAccessible)
                    field.setAccessible(false);
            }
        }

        private void write(ObjectOutput out) throws IOException {
            out.writeInt(fields.size());
            for(String fieldName:fields.keySet()){
                FieldStorage storage = fields.get(fieldName);
                out.writeUTF(fieldName);
                out.writeObject(storage);
            }

            out.writeInt(baseFields.size());
            for(String fieldName:baseFields.keySet()){
                FieldStorage storage = baseFields.get(fieldName);
                out.writeUTF(fieldName);
                out.writeObject(storage);
            }
        }

        private Activation read(ObjectInput in) throws IOException,
                IllegalAccessException,
                NoSuchFieldException,
                ClassNotFoundException, StandardException {
            int numFieldsToRead = in.readInt();
            Class actClass = activation.getClass();
            for(int i=0;i<numFieldsToRead;i++){
                String fieldName = in.readUTF();
                FieldStorage storage = (FieldStorage)in.readObject();
                setField(actClass, fieldName, storage);
            }

            int numBaseFieldsToRead = in.readInt();
            Class baseActClass = getBaseActClass(actClass);
            for(int i=0;i<numBaseFieldsToRead;i++){
                String fieldName = in.readUTF();
                FieldStorage storage = (FieldStorage)in.readObject();
                setField(baseActClass,fieldName,storage);
            }
            return activation;
        }

        private void setField(Class clazz, String fieldName, FieldStorage storage) throws NoSuchFieldException, IllegalAccessException, StandardException {
            Field declaredField = clazz.getDeclaredField(fieldName);
            if(!declaredField.isAccessible()){
                declaredField.setAccessible(true);
                declaredField.set(activation,storage.getValue(activation));
                declaredField.setAccessible(false);
            }else
                declaredField.set(activation,storage.getValue(activation));
        }

    }

    private static Class getBaseActClass(Class actClass) {
        Class baseActClass = actClass;
        while(baseActClass!=null &&!baseActClass.equals(BaseActivation.class))
            baseActClass = baseActClass.getSuperclass();
        if(baseActClass==null) return null;
        return baseActClass;
    }
    private static boolean isQualifierType(Class clazz){
        if(Qualifier.class.isAssignableFrom(clazz)) return true;
        else if(clazz.isArray()){
            return isQualifierType(clazz.getComponentType());
        }else return false;
    }

    private static FieldStorage getFieldStorage(Object o, Class<?> type) {
        for(FieldStorageFactory factory:factories){
            if(factory.isType(type)){
                return factory.create(o,type);
            }
        }
        return null;
    }

    private static interface FieldStorage extends Externalizable{

        Object getValue(Activation context) throws StandardException;
    }

    private static interface FieldStorageFactory<F extends FieldStorage> {
        F create(Object objectToStore, Class type);

        boolean isType(Class type);
    }

    public static class ArrayFieldStorage implements FieldStorage{
        private static final long serialVersionUID = 4l;
        private FieldStorage[] data;
        private Class arrayType;

        @Deprecated
        public ArrayFieldStorage() { }

        public ArrayFieldStorage(Class arrayType,FieldStorage[] fields) {
            this.data = fields;
            this.arrayType = arrayType.isArray()?arrayType.getComponentType(): arrayType;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeUTF(arrayType.getName());
            out.writeInt(data.length);
            for(int i=0;i<data.length;i++){
                FieldStorage storage = data[i];
                out.writeBoolean(storage!=null);
                if(storage!=null)
                   out.writeObject(storage);
            }
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            arrayType = Class.forName(in.readUTF());
            data = new FieldStorage[in.readInt()];
            for(int i=0;i<data.length;i++){
                if(in.readBoolean())
                    data[i] = (FieldStorage)in.readObject();
            }
        }

        @Override
        public Object getValue(Activation context) throws StandardException {
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
        public boolean isType(Class type) {
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
        public boolean isType(Class type) {
            return DataValueDescriptor.class.isAssignableFrom(type);
        }

    }

    public static class DataValueStorage implements FieldStorage {
        private static final long serialVersionUID = 1l;
        private DataValueDescriptor dvd;
        private int type;

        @Deprecated
        public DataValueStorage() { }

        public DataValueStorage(DataValueDescriptor dvd) {
            this.dvd = dvd;
            this.type = dvd.getTypeFormatId();
        }

        @Override
        public Object getValue(Activation context) throws StandardException {
            if(dvd==null)
                dvd= context.getDataValueFactory().getNull(type,0);
            return dvd;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeBoolean(dvd.isNull());
            if(!dvd.isNull())
                out.writeObject(dvd);
            else
                out.writeInt(type);

        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            if(!in.readBoolean())
                dvd = (DataValueDescriptor)in.readObject();
            else
                type = in.readInt();
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
        public boolean isType(Class type) {
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
        public Object getValue(Activation context) throws StandardException {

            ExecRow valueRow;

            DataValueDescriptor[] dvds = (DataValueDescriptor[])data.getValue(context);
            if(isIndexType){
                valueRow = context.getExecutionFactory().getIndexableRow(dvds.length);
            }else
                valueRow = context.getExecutionFactory().getValueRow(dvds.length);

            valueRow.setRowArray((DataValueDescriptor[])data.getValue(context));

            return valueRow;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeBoolean(isIndexType);
            out.writeObject(data);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            this.isIndexType = in.readBoolean();
            this.data = (ArrayFieldStorage)in.readObject();
        }
    }

    private static class SerializableFactory implements FieldStorageFactory<SerializableStorage>{

        @Override
        public SerializableStorage create(Object objectToStore, Class type) {
            return new SerializableStorage((Serializable)objectToStore);
        }

        @Override
        public boolean isType(Class type) {
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
        public Object getValue(Activation context) throws StandardException {
            return data;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(data);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            data = (Serializable)in.readObject();
        }
    }
}
