package com.splicemachine.derby.hbase;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.shared.common.udt.UDTBase;
import com.splicemachine.db.iapi.types.UserType;
import com.splicemachine.derby.utils.marshall.dvd.UDTInputStream;
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
public class ActivationSerializer {

    private static final Logger LOG = Logger.getLogger(ActivationSerializer.class);

    public static void write(Activation source,ObjectOutput out) throws IOException {
        if (classFactory == null) {
            LanguageConnectionContext lcc = source.getLanguageConnectionContext();
            classFactory = lcc.getLanguageConnectionFactory().getClassFactory();
        }
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
            if (classFactory == null) {
                LanguageConnectionContext lcc = destination.getLanguageConnectionContext();
                classFactory = lcc.getLanguageConnectionFactory().getClassFactory();
            }
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
    private static ClassFactory classFactory;

    static{
        factories = Lists.newArrayList();
        factories.add(new DataValueDescriptorFactory());
        factories.add(new ExecRowFactory());
        factories.add(new CachedOpFieldFactory());
        arrayFactory = new ArrayFactory();
        factories.add(arrayFactory);
        factories.add(new CachedOpFieldFactory());
        factories.add(new BooleanFactory());

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
                visit(baseActClass.getDeclaredField("resultSet"),baseFields);
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

        @SuppressWarnings("rawtypes")
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
            if(factory.isType(o, type)){
                return factory.create(o,type);
            }
        }
        return null;
    }

    private static interface FieldStorage extends Externalizable{

        Object getValue(Activation context) throws StandardException;
    }

    private static interface FieldStorageFactory<F extends FieldStorage> {
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
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeBoolean(data);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            this.data = in.readBoolean();
        }

        @Override
        public Object getValue(Activation context) throws StandardException {
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
            if (!dvd.isNull()) {
                boolean useKryo = true;
                if (dvd instanceof UserType && ((UserType) dvd).getObject() instanceof UDTBase) {
                    // If this is a UDT or UDA, do not serialize using kryo
                    useKryo = false;
                    out.writeBoolean(useKryo);
                    ByteArrayOutputStream outputBuffer = new ByteArrayOutputStream();
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputBuffer);
                    objectOutputStream.writeObject(dvd);
                    objectOutputStream.flush();
                    byte[] bytes = outputBuffer.toByteArray();
                    out.writeInt(bytes.length);
                    out.write(bytes);
                    objectOutputStream.close();
                } else {
                    out.writeBoolean(useKryo);
                    out.writeObject(dvd);
                }
            } else {
                out.writeInt(type);
            }
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            if(!in.readBoolean()) {
                if (in.readBoolean()) {
                    dvd = (DataValueDescriptor) in.readObject();
                } else {
                    // This is a UDT or UDA and was not serialized by Kryo
                    int len = in.readInt();
                    byte[] bytes = new byte[len];
                    in.read(bytes, 0, len);
                    ByteArrayInputStream input = new ByteArrayInputStream(bytes);
                    UDTInputStream inputStream = new UDTInputStream(input, classFactory);
                    dvd = (DataValueDescriptor)inputStream.readObject();
                    inputStream.close();
                }

            } else {
                type = in.readInt();
            }
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

        private SpliceOperation operation;

        @Deprecated
        public CachedOpFieldStorage() {
        }

        public CachedOpFieldStorage(SpliceOperation operation) {
            this.operation = operation;
        }

        @Override
        public Object getValue(Activation context) throws StandardException {
            operation.setActivation(context);
            return operation;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(operation);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            this.operation = (SpliceOperation) in.readObject();
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
