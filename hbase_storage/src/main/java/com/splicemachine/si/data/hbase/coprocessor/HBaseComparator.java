package com.splicemachine.si.data.hbase.coprocessor;

import com.splicemachine.primitives.ByteComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.nio.ByteBuffer;

/**
 * @author Scott Fines
 *         Date: 4/20/16
 */
public class HBaseComparator implements ByteComparator{
    public static final HBaseComparator INSTANCE = new HBaseComparator();

    private HBaseComparator(){}

    @Override
    public int compare(byte[] b1,int b1Offset,int b1Length,byte[] b2,int b2Offset,int b2Length){
        return Bytes.compareTo(b1,b1Offset,b1Length,b2,b2Offset,b2Length);
    }

    @Override
    public int compare(ByteBuffer buffer,byte[] b2,int b2Offset,int b2Length){
        byte[] b;
        int bOff;
        int bLen;
        if(buffer.hasArray()){
            b = buffer.array();
            bOff = buffer.position();
            bLen = buffer.remaining();
        }else{
            b=new byte[buffer.remaining()];
            buffer.mark();
            buffer.get(b);
            buffer.reset();
            bOff=0;
            bLen=b.length;
        }
        return compare(b,bOff,bLen,b2,b2Offset,b2Length);
    }

    @Override
    public int compare(ByteBuffer lBuffer,ByteBuffer rBuffer){
        byte[] b;
        int bOff;
        int bLen;
        if(rBuffer.hasArray()){
            b = rBuffer.array();
            bOff = rBuffer.position();
            bLen = rBuffer.remaining();
        }else{
            b=new byte[rBuffer.remaining()];
            rBuffer.mark();
            rBuffer.get(b);
            rBuffer.reset();
            bOff=0;
            bLen=b.length;
        }
        return compare(lBuffer,b,bOff,bLen);
    }

    @Override
    public boolean equals(byte[] b1,int b1Offset,int b1Length,byte[] b2,int b2Offset,int b2Length){
        return compare(b1,b1Offset,b1Length,b2,b2Offset,b2Length)==0;
    }

    @Override
    public boolean equals(byte[] b1,byte[] b2){
        return equals(b1,0,b1.length,b2,0,b2.length);
    }

    @Override
    public boolean equals(ByteBuffer buffer,byte[] b2,int b2Offset,int b2Length){
        byte[] b;
        int bOff;
        int bLen;
        if(buffer.hasArray()){
            b = buffer.array();
            bOff = buffer.position();
            bLen = buffer.remaining();
        }else{
            b=new byte[buffer.remaining()];
            buffer.mark();
            buffer.get(b);
            buffer.reset();
            bOff=0;
            bLen=b.length;
        }
        return equals(b,bOff,bLen,b2,b2Offset,b2Length);
    }

    @Override
    public boolean equals(ByteBuffer lBuffer,ByteBuffer rBuffer){
        byte[] b;
        int bOff;
        int bLen;
        if(rBuffer.hasArray()){
            b = rBuffer.array();
            bOff = rBuffer.position();
            bLen = rBuffer.remaining();
        }else{
            b=new byte[rBuffer.remaining()];
            rBuffer.mark();
            rBuffer.get(b);
            rBuffer.reset();
            bOff=0;
            bLen=b.length;
        }
        return equals(lBuffer,b,bOff,bLen);
    }

    @Override
    public boolean isEmpty(byte[] stop){
        return stop==null||stop.length<=0;
    }

    @Override
    public int compare(byte[] o1,byte[] o2){
        return compare(o1,0,o1.length,o2,0,o2.length);
    }
}
