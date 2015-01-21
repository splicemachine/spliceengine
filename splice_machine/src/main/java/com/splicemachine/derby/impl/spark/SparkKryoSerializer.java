package com.splicemachine.derby.impl.spark;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.spark.serializer.DeserializationStream;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.reflect.ClassTag;

import java.io.EOFException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dgomezferro on 4/17/14.
 */
public class SparkKryoSerializer extends Serializer {

    @Override
    public SerializerInstance newInstance() {
        return new KryoSerializerInstance(SpliceSparkKryoRegistry.getInstance().get());
    }
}

class KryoSerializerInstance extends SerializerInstance {

    private static final int INITIAL_BUFFER_SIZE = 2 * 1024 * 1024;
    private Kryo kryo;
    private Output out = new Output(INITIAL_BUFFER_SIZE, -1);
    private Input in = new Input();

    public KryoSerializerInstance(Kryo kryo) {
        this.kryo = kryo;
    }

    @Override
    public <T> ByteBuffer serialize(T t, ClassTag<T> tClassTag) {
        out.clear();
        kryo.writeClassAndObject(out, t);
        return ByteBuffer.wrap(out.toBytes());
    }

    @Override
    public <T> T deserialize(ByteBuffer byteBuffer, ClassTag<T> tClassTag) {
        in.setBuffer(byteBuffer.array());
        return (T) kryo.readClassAndObject(in);
    }

    @Override
    public <T> T deserialize(ByteBuffer byteBuffer, ClassLoader classLoader, ClassTag<T> tClassTag) {
        ClassLoader oldClassLoader = kryo.getClassLoader();
        kryo.setClassLoader(classLoader);
        in.setBuffer(byteBuffer.array());
        T obj = (T) kryo.readClassAndObject(in);
        kryo.setClassLoader(oldClassLoader);
        return obj;
    }

    @Override
    public SerializationStream serializeStream(OutputStream outputStream) {
        return new KryoSerializationStream(kryo, outputStream);
    }

    @Override
    public DeserializationStream deserializeStream(InputStream inputStream) {
        return new KryoDeserializationStream(kryo, inputStream);
    }

//    @Override
//    public <T> ByteBuffer serializeMany(Iterator<T> tIterator, ClassTag<T> tClassTag) {
//        return null;
//    }
//
//    @Override
//    public Iterator<Object> deserializeMany(ByteBuffer byteBuffer) {
//        List<Object> objs = new ArrayList<Object>();
//        while (byteBuffer.hasRemaining()) {
//            objs.add(deserialize(byteBuffer, null));
//        }
//        return JavaConversions.asScalaIterator(objs.iterator());
//    }
}

class KryoSerializationStream extends SerializationStream {

    private final Kryo kryo;
    private final Output output;

    public KryoSerializationStream(Kryo kryo, OutputStream outputStream) {
        this.kryo = kryo;
        this.output = new Output(outputStream);
    }

    @Override
    public <T> SerializationStream writeObject(T t, ClassTag<T> tClassTag) {
        kryo.writeClassAndObject(output, t);
        return this;
    }

    @Override
    public void flush() {
        output.flush();
    }

    @Override
    public void close() {
        output.close();
    }

    @Override
    public <T> SerializationStream writeAll(Iterator<T> tIterator, ClassTag<T> tClassTag) {
        while (tIterator.hasNext()) {
            writeObject(tIterator.next(), null);
        }
        return this;
    }
}

class KryoDeserializationStream extends DeserializationStream {

    private final Input input;
    private final Kryo kryo;

    public KryoDeserializationStream(Kryo kryo, InputStream inputStream) {
        this.kryo = kryo;
        this.input = new Input(inputStream);
    }

    @Override
    public <T> T readObject(ClassTag<T> tClassTag) {
        try {
            return (T) kryo.readClassAndObject(input);
        } catch (KryoException e) {
            if (e.getMessage().toLowerCase().contains("buffer underflow")) {
                KryoDeserializationStream.<RuntimeException>throwException(new EOFException());
                return null; // unreachable
            } else {
                throw e;
            }
        }
    }

    @Override
    public void close() {
        // Kryo's Input automatically closes the input stream it is using.
        input.close();
    }

    @Override
    public Iterator<Object> asIterator() {
        return JavaConversions.asScalaIterator(new KryoIterator());
    }

    @SuppressWarnings("unchecked")
    public static <E extends Throwable> void throwException(Throwable exception) throws E
    {
        throw (E) exception;
    }

    private class KryoIterator implements java.util.Iterator<Object> {
        private Object next = null;

        KryoIterator() {
            try {
                next = readObject(null);
            } catch (Exception e) {
                if (e instanceof EOFException) {
                    next = null;
                } else {
                    KryoDeserializationStream.<RuntimeException>throwException(e);
                }
            }
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public Object next() {
            Object result = next;
            try {
                next = readObject(null);
            } catch (Exception e) {
                if (e instanceof EOFException) {
                    next = null;
                } else {
                    KryoDeserializationStream.<RuntimeException>throwException(e);
                }
            }
            return result;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}