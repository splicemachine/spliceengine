package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.utils.RingBuffer;
import org.apache.derby.iapi.sql.execute.ExecRow;


/**
 * Implements a HashTable for ExecRows specifically.
 *
 * This is effectively a hash from some hash keys to a list of ExecRows which have the same
 * hashing keys
 * @author Scott Fines
 * Date: 7/22/14
 */
@SuppressWarnings("ForLoopReplaceableByForEach")
public class ExecRowHashTable {
    public void markAllBuffers() {
        for(int i=0;i<buffer.length;i++){
            RingBuffer<ExecRow> buf = buffer[i];
            if(buf!=null)
                buf.mark();
        }
    }

    public interface RowHasher{
        int hash(ExecRow row);
        boolean equalsOnHash(ExecRow left, ExecRow right);
    }

    private RingBuffer<ExecRow>[] buffer;
    private int size;
    @SuppressWarnings("FieldCanBeLocal")
    private float fillFactor = 0.75f;
    private final RowHasher entryHasher;
    private final RowHasher lookupHasher;

    @SuppressWarnings("unchecked")
    public ExecRowHashTable(int size,RowHasher entryHasher,RowHasher lookupHasher) {
        int s = 1;
        while(s<size)
            s<<=1;
        buffer = new RingBuffer[s];
        this.entryHasher = entryHasher;
        this.lookupHasher = lookupHasher;
    }

    public void add(ExecRow row){
        int slot = entryHasher.hash(row);
        int pos = slot & (buffer.length-1);

        int i;
        for(i=0;i<buffer.length;i++){
            RingBuffer<ExecRow> list = buffer[pos];
            if(list==null){
                list = new RingBuffer<ExecRow>(1);
                buffer[pos] = list;
                incrementSize(entryHasher);
            }

            if(list.size()<=0){
                list.add(row);
                break;
            }else{
                ExecRow other = list.peek();
                if(entryHasher.equalsOnHash(row,other)){
                    if(list.isFull()){
                        list.expand();
                    }
                    list.add(row);
                    break;
                }
            }
            pos = (pos+1) & (buffer.length-1);
        }

    }

    private void incrementSize(RowHasher hasher) {
        size++;
        if(size>=fillFactor*buffer.length){
            resizeTable(hasher);
        }
    }

    @SuppressWarnings("unchecked")
    private void resizeTable(RowHasher hasher) {
        RingBuffer<ExecRow>[] newBuffer = new RingBuffer[2*buffer.length];
        for(int i=0;i<buffer.length;i++){
            RingBuffer<ExecRow> list = buffer[i];
            if(list==null || list.size()<=0) continue;
            put(newBuffer,list,hasher);
        }
        buffer = newBuffer;
    }

    private void put(RingBuffer<ExecRow>[] buf, RingBuffer<ExecRow> list, RowHasher hasher) {
        ExecRow first = list.peek();
        int pos = hasher.hash(first) & (buffer.length-1);
        for(int i=0;i<buffer.length;i++){
            RingBuffer<ExecRow> next = buf[pos];
            if(next==null){
                buf[pos] = list;
                return;
            }
            pos = (pos+1) & (buffer.length-1);
        }
    }

    public RingBuffer<ExecRow> get(ExecRow row){
        int pos = lookupHasher.hash(row) & (buffer.length-1);
        for(int i=0;i<buffer.length;i++){
            RingBuffer<ExecRow> lookup = buffer[pos];
            if(lookup!=null && lookup.size()>0){
                ExecRow first = lookup.peek();
                if(lookupHasher.equalsOnHash(row,first))
                    return lookup;
            }
            pos = (pos+1) & (buffer.length-1);
        }
        return null;
    }

    int size(){
        return size;
    }

    @SuppressWarnings("unchecked")
    void clear(){
        buffer = new RingBuffer[buffer.length]; //release the other array to be collected
        size = 0;
    }
}
