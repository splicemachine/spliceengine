package com.splicemachine.storage;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.ObjectArrayList;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.Provider;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author Scott Fines
 * Created on: 7/8/13
 */
public class EntryPredicateFilter {
    private static final Logger LOG = Logger.getLogger(EntryPredicateFilter.class);
    public static EntryPredicateFilter EMPTY_PREDICATE = new EntryPredicateFilter(new BitSet(), new ObjectArrayList<Predicate>());
    private BitSet fieldsToReturn;
    private ObjectArrayList<Predicate> valuePredicates;
    private boolean returnIndex;
    private BitSet predicateColumns;
    private int[] columnOrdering;
    private int[] format_ids;

		private long rowsFiltered = 0l;
		private EntryAccumulator accumulator;

		public static EntryPredicateFilter emptyPredicate(){ return EMPTY_PREDICATE; }

    public EntryPredicateFilter(BitSet fieldsToReturn, ObjectArrayList<Predicate> predicates){
        this(fieldsToReturn, predicates,true);
    }

    public EntryPredicateFilter(BitSet fieldsToReturn, ObjectArrayList<Predicate> predicates,boolean returnIndex){
        this.fieldsToReturn = fieldsToReturn;
        this.valuePredicates = predicates;
        this.returnIndex=returnIndex;
    }

    public EntryPredicateFilter(BitSet fieldsToReturn, ObjectArrayList<Predicate> predicates,boolean returnIndex,
                                int[] columnOrdering, int[] format_ids){
        this.fieldsToReturn = fieldsToReturn;
        this.valuePredicates = predicates;
        this.returnIndex=returnIndex;
        this.columnOrdering = columnOrdering;
        this.format_ids = format_ids;
    }

    public EntryPredicateFilter(BitSet fieldsToReturn, ObjectArrayList<Predicate> predicates,
                                int[] columnOrdering, int[] format_ids){
        this.fieldsToReturn = fieldsToReturn;
        this.valuePredicates = predicates;
        this.columnOrdering=columnOrdering;
        this.format_ids = format_ids;
    }


		public boolean match(Indexed index,
												 Provider<MultiFieldDecoder> decoderProvider,
												 EntryAccumulator accumulator) throws IOException{
				BitSet remainingFields = accumulator.getRemainingFields();

				MultiFieldDecoder decoder = decoderProvider.get();
				for(int encodedPos =index.nextSetBit(0);
						remainingFields.cardinality()>0 && encodedPos>=0&&encodedPos<=remainingFields.length();
						encodedPos=index.nextSetBit(encodedPos + 1)){
						if(!remainingFields.get(encodedPos)){
								skipField(decoder,encodedPos,index);
								continue;
						}

						int offset = decoder.offset();

						byte[] array = decoder.array();
						if(offset>array.length){
                /*
                 * It may seem odd that this case can happen, but it occurs when the scan
                 * doesn't provide information about how many columns it expects back, and
                 * the bit index is an AllFullBitIndex (This is the case when you want to return
                 * everything, and everything is present). In that case, there is nothing to stop
                 * this from looping off the end of the data array (which, in fact, is the correct
                 * behavior when you want to return everything), so we have to check and make sure that
                 * there actually IS data to add before adding. In that case, we've finished, so
                 * tell the accumulator
                 */
								if(fieldsToReturn==null||fieldsToReturn.isEmpty())
										accumulator.complete();
								return true;
						}
						skipField(decoder, encodedPos, index);


						int limit = decoder.offset()-1-offset;
						if(limit<=0){
								//we have an implicit null field
								limit=0;
						}else if(offset+limit>array.length){
								limit = array.length-offset;
						}

						Object[] buffer = valuePredicates.buffer;
						int ibuffer = valuePredicates.size();
						for (int i =0; i<ibuffer; i++) {
								if(((Predicate)buffer[i]).applies(encodedPos) && !((Predicate)buffer[i]).match(encodedPos,array, offset,limit)){
										rowsFiltered++;
										return false;
								}
						}
						accumulate(index, encodedPos, accumulator, array, offset, limit);
				}
				return true;
		}


		public boolean match(EntryDecoder entry,EntryAccumulator accumulator) throws IOException {
				return match(entry.getCurrentIndex(),entry, accumulator);
    }

    public BitSet getCheckedColumns(){
        if(predicateColumns==null){
            predicateColumns = new BitSet();
            Object[] buffer = valuePredicates.buffer;
            int ibuffer = valuePredicates.size();
            for (int i =0; i<ibuffer; i++) {
                ((Predicate)buffer[i]).setCheckedColumns(predicateColumns);
            }
        }
        // exclude primary key columns
        if (columnOrdering != null && columnOrdering.length > 0) {
            for (int col:columnOrdering) {
                predicateColumns.clear(col);
            }
        }
        return predicateColumns;
    }

    public boolean checkPredicates(ByteSlice buffer,int position){
        Object[] vpBuffer = valuePredicates.buffer;
        int ibuffer = valuePredicates.size();
        for (int i =0; i<ibuffer; i++) {
            Predicate predicate = (Predicate) vpBuffer[i];
            if(!predicate.applies(position))
                continue;
            if(predicate.checkAfter()){
                if(buffer!=null){
                    if(!predicate.match(position,buffer.array(),buffer.offset(),buffer.length()))
                        return false;
                }else{
                    if(!predicate.match(position,null,0,0))
                        return false;
                }
            }
        }
        return true;
    }

    public void rowReturned(){
        //no-op
    }

    public void reset(){
        Object[] vpBuffer = valuePredicates.buffer;
        int ibuffer = valuePredicates.size();
        for (int i =0; i<ibuffer; i++) {
        	((Predicate)vpBuffer[i]).reset();
        	
        }
    }

    public EntryAccumulator newAccumulator(){
				if(accumulator==null){
						return new ByteEntryAccumulator(this,returnIndex,fieldsToReturn);
//						if(fieldsToReturn.isEmpty())
//								accumulator = new AlwaysAcceptEntryAccumulator(this,returnIndex);
//						else
//								accumulator = new SparseEntryAccumulator(this,fieldsToReturn,returnIndex);
				}
				return accumulator;
    }

		public long getRowsOutput(){
				if(accumulator==null) return 0;
				else return accumulator.getFinishCount();
		}

		public long getRowsFiltered(){ return rowsFiltered; }

    public byte[] toBytes() {
        //if we dont have any distinguishing information, just send over an empty byte array
        if(fieldsToReturn.length()==0 && valuePredicates.size()<=0 && !returnIndex)
            return new byte[]{};

        /*
         * Format is as follows:
         * BitSet bytes
         * 1-byte returnIndex
         * n-bytes predicates
         */
        byte[] bitSetBytes = BytesUtil.toByteArray(fieldsToReturn);
        byte[] predicates = Predicates.toBytes(valuePredicates);
        int size = predicates.length+bitSetBytes.length+1;
        size += 4; // 4 bytes for length of columnOrdering
        if (columnOrdering != null) {
            size += 4 * columnOrdering.length;
        }
        size += 4; // 4 bytes for number of format ids
        if (format_ids != null) {
            size += 4 * format_ids.length;
        }

        byte[] finalData = new byte[size];
        System.arraycopy(bitSetBytes,0,finalData,0,bitSetBytes.length);
        finalData[bitSetBytes.length] = returnIndex? (byte)0x01: 0x00;
        System.arraycopy(predicates,0,finalData,bitSetBytes.length+1,predicates.length);
        int index = bitSetBytes.length + 1 + predicates.length;
        int n = columnOrdering != null ? columnOrdering.length : 0;
        BytesUtil.intToBytes(n, finalData, index);
        index += 4;
        if (columnOrdering != null) {
            for (int col:columnOrdering) {
                BytesUtil.intToBytes(col, finalData, index);
                index += 4;
            }
        }
        n = format_ids != null ? format_ids.length : 0;
        BytesUtil.intToBytes(n, finalData, index);
        index += 4;
        if (format_ids != null) {
            for (int id:format_ids) {
                BytesUtil.intToBytes(id, finalData, index);
                index += 4;
            }
        }
        assert (index == size);

        return finalData;
    }

    public static EntryPredicateFilter fromBytes(byte[] data) throws IOException {
        if(data==null||data.length==0) return EMPTY_PREDICATE;

        Pair<BitSet,Integer> fieldsToReturn = BytesUtil.fromByteArray(data,0);
        boolean returnIndex = data[fieldsToReturn.getSecond()] > 0;
        Pair<ObjectArrayList<Predicate>,Integer> predicates = Predicates.allFromBytes(data,fieldsToReturn.getSecond()+1);
        int index = predicates.getSecond();
        int[] columnOrdering = null;
        int[] format_ids = null;
        //read columnOrdering length
        int n = BytesUtil.bytesToInt(data, index);
        index += 4;
        if (n > 0) {
            columnOrdering = new int[n];
            for (int i = 0; i < n; ++i) {
                columnOrdering[i] = BytesUtil.bytesToInt(data, index);
                index += 4;
            }
        }

        // read length of format_ids
        n = BytesUtil.bytesToInt(data, index);
        index += 4;
        if (n > 0) {
            format_ids = new int[n];
            for (int i = 0; i < n; ++i) {
                format_ids[i] = BytesUtil.bytesToInt(data, index);
                index += 4;
            }
        }
        assert (index == data.length);
        return new EntryPredicateFilter(fieldsToReturn.getFirst(),predicates.getFirst(),returnIndex, columnOrdering, format_ids);
    }

    public ObjectArrayList<Predicate> getValuePredicates() {
        return valuePredicates;
    }


		private void skipField(MultiFieldDecoder decoder, int position, Indexed index) {
				if(index.isScalarType(position)){
						decoder.skipLong();
				}else if(index.isFloatType(position)){
						decoder.skipFloat();
				}else if(index.isDoubleType(position))
						decoder.skipDouble();
				else
						decoder.skip();
		}

		private void accumulate(Indexed index, int position, EntryAccumulator accumulator, byte[] buffer, int offset, int length) {
				if(index.isScalarType(position)){
						accumulator.addScalar(position,buffer,offset,length);
				}else if(index.isFloatType(position)){
						accumulator.addFloat(position,buffer,offset,length);
				}else if(index.isDoubleType(position))
						accumulator.addDouble(position,buffer,offset,length);
				else
						accumulator.add(position,buffer,offset,length);
		}
}
