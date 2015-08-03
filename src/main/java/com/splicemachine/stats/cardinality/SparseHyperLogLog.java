package com.splicemachine.stats.cardinality;

import com.splicemachine.encoding.Encoder;
import com.splicemachine.hash.Hash64;
import com.splicemachine.primitives.MoreArrays;
import com.splicemachine.stats.DoubleFunction;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 * A Sparsely-stored, bias-adjusted HyperLogLog Counter.
 * <p/>
 * <p>This implementation takes advantage of small cardinalities to both
 * be more memory efficient and potentially to increase the overall accuracy of
 * the estimation.</p>
 * <p/>
 * <p>When the observed cardinality is very small, then most registers are empty,
 * resulting in a considerable waste of space, particularly for higher accuracy. To
 * avoid this situation, this implementation begins by storing data in a sparse integer
 * array, where the first few bits of the integer stores the index of the register,
 * and the remaining bits store the value. Of course, if the number of non-empty
 * registers grows beyond a certain point, the sparse representation becomes less
 * memory-efficient than a dense one, so the implementation will automatically switch
 * to a dense implementation after that point.</p>
 * <p/>
 * <p>Our sparse encoding is as follows. Let {@code p} be the precision desired. Then,
 * we know that {@code idx} is the register to which the value belongs. Set {@code p(w)}
 * to be the leftmost 1-bit in the hashed value.</p>
 * <p/>
 * <p>We know from algorithm analysis that we can store {@code p(w)} in 6 bits, but we require
 * {@code p} bits to store {@code idx}. Thus, we need at least {@code p+6} bits to store the
 * sparse representation.</p>
 * <p/>
 * <p>Since {@code p+6} is not likely to be a full word of any size, we will end up wasting at least
 * some bits. Why not waste them storing real data? Instead of storing p bits for the index, store
 * {@code idx' = wordSize-6} bits of the hash. This results in more registers, and thus better accuracy,
 * without using any storage space that we weren't using already.</p>
 * <p/>
 * <p>Furthermore, we now have a situation where {@code p(w)} may already stored in the {@code idx'} value
 * (if there is a 1-bit contains somewhere in the least significant {wordSize-p}). In that case, we don't
 * need to store the value at all, we know it already.  Thus, we end up with a format of
 * <p/>
 * idx' | p(w)
 * <p/>
 * if we needed to store {@code p(w)}. Otherwise, we end up with
 * <p/>
 * idx' | 0
 * <p/>
 * Thus, for an int, we have 26-bits for {@code idx'} and 6 bits for {@code p(w)}.</p>
 * <p/>
 * <p>When we go to add an entry, we first compose an int with the value. If the least-significant bit is 0, then
 * all we need to do is ensure that the register is present, since all values to that bucket will have the same
 * p(w). Otherwise, we will need to do a comparison on the 6-bits {@code p(w)} value to keep the maximum.</p>
 * <p/>
 * <p>Finally, we keep this array sorted, which allows us to efficiently convert between the sparse and
 * dense representations when necessary. When we go to convert to a dense representation, however, we will
 * down-convert the precision to be whatever the user calls for (so if the user asks for precision 10, we will
 * use a dense representation with precision 10, not 25), so some precision is lost at that point. However, no
 * precision is lost if the sparse representation is kept all the way through.</p>
 * <p/>
 * <p>In truth, one notices that, if the sparse representation is maintained throughout all updates and an estimate
 * is asked for, then the cardinality is low enough that fewer than {@code 3*2^(precision-6)} registers were filled,
 * which is generally well below the empirical threshold at which it's more accurate to use linear counting anyway.
 * Thus, we can immediately perform linear counting without needing to perform the hyperloglog estimate at all.</p>
 * <p/>
 * <p>Updates to a sparse structure are relatively expensive, however. To this end, this implementation buffers
 * results up to a threshold before flushing those changes to the sparse structure in a single pass. By default, this
 * buffer threshold is set to be 25% of the maximum sparse register size (25% of {@code 2^(precision+3)}</p
 *
 * @author Scott Fines
 *         Date: 7/21/15
 */
public class SparseHyperLogLog extends BaseBiasAdjustedHyperLogLogCounter{
    private int[] sparseArray;
    private final int maxSparseSize;
    private boolean isSparse;
    private int sparseSize = 0;

    private byte[] denseRegisters;

    private int[] buffer;
    private int bufferSize;

    public SparseHyperLogLog(int size,Hash64 hashFunction){
        this(size,hashFunction,HyperLogLogBiasEstimators.biasEstimate(size));
    }

    public SparseHyperLogLog(int precision,
                             Hash64 hashFunction,
                             DoubleFunction biasAdjuster){
        super(precision,hashFunction,biasAdjuster);

        int denseSize = 8*(1<<(precision));
        /*
         * We want a sparse representation to always be smaller than the dense size--otherwise,
         * it's cheaper to just use the dense implementation. Since the sparse implementation uses
         * a buffer and a sparse array, we have
         *
         * buffer.length+sparseArray.length<denseSize/4
         *
         * will satisfy our goals. We divvy up the two by making the buffer equal to 1/4, and the sparse
         * array 3/4 of the total size.
         *
         * Note, if the size is really small, we don't care so much if we exactly save--nothing we do is going to make
         * it larger
         */
        int maxTotalSize = denseSize>>2;
        int bSize;
        if(maxTotalSize>4)
            bSize = maxTotalSize>>2;
        else bSize = maxTotalSize;

        this.maxSparseSize =Math.min(maxTotalSize,3*bSize);
        this.isSparse=true;
        this.buffer = new int[bSize];
        this.bufferSize=0;
    }

    private SparseHyperLogLog(int precision,
                              Hash64 hashFunction,
                              DoubleFunction biasAdjuster,
                              int sparseSize,
                              int[] sparseArray){
        super(precision,hashFunction,biasAdjuster);
        this.sparseArray = sparseArray;
        this.sparseSize = sparseSize;
        int denseSize = 8*(1<<(precision));
        /*
         * We want a sparse representation to always be smaller than the dense size--otherwise,
         * it's cheaper to just use the dense implementation. Since the sparse implementation uses
         * a buffer and a sparse array, we have
         *
         * buffer.length+sparseArray.length<denseSize/4
         *
         * will satisfy our goals. We divvy up the two by making the buffer equal to 1/4, and the sparse
         * array 3/4 of the total size.
         *
         * Note, if the size is really small, we don't care so much if we exactly save--nothing we do is going to make
         * it larger
         */
        int maxTotalSize = denseSize>>2;
        int bSize;
        if(maxTotalSize>4)
            bSize = maxTotalSize>>2;
        else bSize = maxTotalSize;

        buffer = new int[bSize];
        this.maxSparseSize = 3*bSize;
        bufferSize = 0;
        this.isSparse = true;
    }

    private SparseHyperLogLog(int precision,
                             Hash64 hashFunction,
                             DoubleFunction biasAdjuster,
                             byte[] denseRegisters){
        super(precision,hashFunction,biasAdjuster);
        this.denseRegisters =Arrays.copyOf(denseRegisters,denseRegisters.length);
        this.maxSparseSize = 0;
        this.isSparse = false;
    }

    @Override
    public BaseLogLogCounter getClone(){
        if(isSparse)
            return new SparseHyperLogLog(precision,hashFunction,biasAdjuster,sparseSize,sparseArray);
        else
            return new SparseHyperLogLog(precision,hashFunction,biasAdjuster,denseRegisters);
    }

    @Override
    protected void updateRegister(int register,int value){
        byte curr = denseRegisters[register];
        if(curr<value)
            denseRegisters[register] = (byte)(value &0xff);
    }

    @Override
    public void merge(BaseLogLogCounter otherCounter){
        mergeBuffer();
        if(!(otherCounter instanceof SparseHyperLogLog) || !this.isSparse){
            /*
             * If the other counter isn't a SparseHyperLogLog, then convert this to dense,
             * and then defer to the default behavior.
             *
             * This condition is also applicable if we've already converted to a dense encoding,
             * because the algorithm becomes the same as the default under those cicumstances
             */
            convertToDense();
            super.merge(otherCounter);
            return;
        }

        SparseHyperLogLog other = (SparseHyperLogLog)otherCounter;
        other.mergeBuffer(); //ensure that the other has merged its buffer
        if(other.isSparse){
            int[] otherSparse=other.sparseArray;
            int otherSparseSize=other.sparseSize;
            for(int i=0;i<otherSparseSize;i++){
                int r=otherSparse[i];
                if(this.isSparse)
                    buffer(r);
                else
                    insertIntoDense(r);
            }
        }else{
            /*
             * The other element is large enough that it determined to be better
             * off as a dense version, which means that we will also be that large after
             * merging, so we may as well convert to dense and defer to default behavior
             */
            convertToDense();
            super.merge(other);
        }
    }

    @Override
    protected int getRegister(int register){
        return denseRegisters[register];
    }


    @Override
    public long getEstimate(){
        if(isSparse)
            mergeBuffer();

        if(isSparse){
            //count zero registers and use linear counting for our estimate
            int mP=(1<<PRECISION_BITS);
            int numZeroRegisters=mP-sparseSize; //the number of missing entries is the number of zero registers
            return (long)(mP*Math.log((double)mP/numZeroRegisters));
        }else
            return super.getEstimate();
    }

    private static final int PRECISION_BITS = 26;
    private static final int INT_SPARSE_SHIFT = Integer.SIZE-PRECISION_BITS;
    private static final int SPARSE_SHIFT = Long.SIZE-PRECISION_BITS;

    @Override
    protected void doUpdate(long hash){
        if(!isSparse){
            super.doUpdate(hash);
            return;
        }
        int register = ((int)(hash>>>SPARSE_SHIFT));
        int r = register<<INT_SPARSE_SHIFT;
        if((r<<precision)==0){
            long w = hash<<precision;
            int p=w==0?1:Long.numberOfLeadingZeros(w)+1;
            r |=p;
        }

        buffer(r);
    }


    /* ****************************************************************************************************************/
    /*private helper methods*/
    private void buffer(int r){
        buffer[bufferSize]= r;
        bufferSize++;
        if(bufferSize==buffer.length)
            mergeBuffer();
    }

    private void mergeBuffer(){
        /*
         * This merges the buffer into the sparse array, using a relatively complicated approach.
         *
         * First, if the total space occupied exceeds the maximum size allotted to the sparse array,
         * then we convert to a dense encoding (since it will use less space).
         *
         * If the space is acceptable, then we short-circuit the firs buffer load by just making the buffer
         * the array. Otherwise, we use an "internal merge" strategy, which does a combination of merging and
         * de-duplicating the buffer into the sparse array.
         */
        if(bufferSize==0){
            //Safety valve--we don't expect to see this, but just in case protect against it
            return;
        }else if(sparseSize==0){
            /*
             * Short-circuit the first buffer merging. Since we know that there is nothing in the sparse
             * array, throwing it away is not a problem, so just point the sparse array to the buffer and return.
             * (In practice, sparseSize==0 implies that sparseArray is null anyway, so we are really just allocating
             * a single new array).
             */
            sortBuffer();
            sparseArray=buffer;
            sparseSize=bufferSize;
            buffer=new int[buffer.length];
        }else if(sparseSize+bufferSize>=maxSparseSize){
            /*
             * The space that the sparse representation occupies exceeds the space requirements for the dense
             * encodings, so convert to a dense. This process has a tendency to lose accuracy (since it converts
             * from a p of PRECISION_BITS to the user-configured p instead), but it optimal from a space
             * utilization perspective.
             */
            convertToDense();
            for(int i=0;i<bufferSize;i++){
                insertIntoDense(buffer[i]);
            }
            buffer = null; //null out to help the garbage collector
        } else{
            //perform the raw merge between buffer and sparse array
            mergeInternal();
        }
        bufferSize=0;
    }

    private void mergeInternal(){
        /*
         * Merges the buffer into the sparse array. This uses a variant on sort-merging, but it *does* allocate
         * a new array to contain the results of the merge (it does not attempt an in-place merging). This is done
         * for performance reasons, as well as the likelihood that sparseArray isn't big enough to hold all the buffer
         * elements anyway.
         */
        sortBuffer();

        int bPos = 0;
        int sPos = 0;
        int dPos = 0;
        int[] dest = new int[sparseSize+bufferSize];
        int buff = 0;
        int buffReg = 0;
        int s = 0;
        int sReg = 0;

        /*
         * We fill the destination array by merging together the buffer and the sparse array.
         *
         * Each of the two arrays can be sorted, and we know a few additional facts:
         *
         * 1. sparse array has no duplicate entries--there is one entry per register
         * 2. Buffer may have duplicate entries for a given register.
         *
         * So we have to compare buffer with sparse, and ALSO with the previously inserted
         * entry in case we have a duplicate.
         *
         * We do this using three positional counters: dPos is the current position in the destination
         * array, bPos is the current position in the buffer, and sPos is the current position in the sparse
         * array.
         */
        int lastDest = 0;
        int lastDestReg = 0;
        do{
            /*
             * There are the following situations:
             *
             * 1. buffReg < sparseReg => insert buff, and move bPos forward
             * 2. buffReg == sparseReg => choose based on max p value, and move bPos and sPos forward
             * 3. buffReg > sparseReg => insert sparse, and move sparse forward
             *
             * Once those three are chosen, we compare against lastDest. We know that lastDest <= element,
             * since we maintain that in the first step. Therefore, we have the following scenarios:
             *
             * A. lastDestReg == elementReg => choose based on max p value. If element p > lastDest p then replace lastDest with
             * element and *dont move dPos forward*
             * B. lastDestReg < elementReg => insert element and move dPos forward
             */
            int e;
            int eReg;
            if(bPos>=bufferSize){
                /*
                 * There are no more buffer elements, so we can actually short-circuit here.
                 *
                 * Because there are no more buffer elements, we will copy the remainder in from the
                 * sparse array. However, since sparseArray has no duplicates, we can just copy those
                 * values in directly and return.
                 */
                System.arraycopy(sparseArray,sPos,dest,dPos,sparseSize-sPos);
                dPos +=(sparseSize-sPos);
                break;
            }else if(buff==0){
                //we need to advance b
                buff = buffer[bPos];
                buffReg = buff>>>INT_SPARSE_SHIFT;
            }

            if(sPos >=sparseSize){
                /*
                 * We are out of sparse elements, so fill the remainder from the buffer. We cannot
                 * short-circuit here, as buffer may contain duplicates, instead, we just
                 * set e to be buff
                 */
                e = buff;
                eReg = buffReg;
                buff=0;
                bPos++;
            }else{
                if(s==0){
                    //we need to advance s
                    s=sparseArray[sPos];
                    sReg=s>>>INT_SPARSE_SHIFT;
                }

                if(buffReg<sReg){
                    e=buff;
                    eReg=buffReg;
                    buff=0;
                    bPos++;
                }else if(buffReg>sReg){
                    e=s;
                    eReg=sReg;
                    s=0;
                    sPos++;
                }else{
                    /*
                     * The two values are equal on the first 25 bits. This has one of the following cases:
                     *
                     * 1. p contained in the first 25 bits of b and s
                     * 2. p contained in the first 25 bits of b, not s
                     * 3. p contained in the first 25 bits of s, not b
                     * 4. p contained in neither s or p's first 25 bits
                     *
                     * Cases 2 and 3 cannot happen, because the two values match on the first 25 bits. Thus,
                     * if p is in one, it's in both (Case 1), and if it's not in one, it's not in the other (Case 4),
                     * so we only need to deal with Cases 1 and 4.
                     *
                     * in Case 1, we know that b and s are actually identical, so it doesn't matter which one we
                     * pick. In this case, pick the buffer value cause why not.
                     *
                     * In case 4, choose the element with the largest p value (v & 0x3F).
                     */
                    int bP=buff&0x3F;
                    if(bP==0){
                        //p is in the first 25 bits, hooray! we are golden
                        e=buff;
                        eReg=buffReg;
                    }else{
                        int sP=s&0x3F;
                        if(bP>sP){
                            //choose b
                            e=buff;
                            eReg=buffReg;
                        }else{
                            //choose s
                            e=s;
                            eReg=sReg;
                        }
                    }
                    buff=0;
                    bPos++;
                    s=0;
                    sPos++;
                }
            }

            //compare to last inserted value
            if(lastDestReg<eReg){
                //add to the end and advance
                dest[dPos] = e;
                dPos++;
                lastDest = e;
                lastDestReg = eReg;
            }else if(lastDestReg==eReg){
                /*
                 * The two values are equal on the first 25 bits. This has one of the following cases:
                 *
                 * 1. p contained in the first 25 bits of b and s
                 * 2. p contained in the first 25 bits of b, not s
                 * 3. p contained in the first 25 bits of s, not b
                 * 4. p contained in neither s or p's first 25 bits
                 *
                 * Cases 2 and 3 cannot happen, because the two values match on the first 25 bits. Thus,
                 * if p is in one, it's in both (Case 1), and if it's not in one, it's not in the other (Case 4),
                 * so we only need to deal with Cases 1 and 4.
                 *
                 * in Case 1, we know that b and s are actually identical, so it doesn't matter which one we
                 * pick. In this case, pick the buffer value cause why not.
                 *
                 * In case 4, choose the element with the largest p value (v & 0x3F).
                 */
                int dP = lastDest & 0x3F;
                int eP = e & 0x3F;
                if(dP<eP){
                    //we have a greater element, so replace without advancing d
                    dest[dPos] = eP;
                    lastDest = e;
                    lastDestReg = eReg;
                }
                //if the if-check isn't matched, d and e are identical, so ignore e and move on
            }
        }while(bPos<bufferSize||sPos<sparseSize);

        sparseArray = dest;
        sparseSize = dPos;
    }


    private void sortBuffer(){
        /*
         * We want to sort in ascending register order. Registers fall between 0 and 2^PRECISION_BITS, which
         * means that they are always positive. However, because we left-shift the registers to make room for p
         * values, we sometimes make the stored values negative. Thus, we have a potential change in sign, which
         * makes sorting registers in normal ascending order incorrect (that is, there exists two registers R1 and
         * R2 such that R1 < R2, but the encoded numbers N1 and N2 are such that N2 < N1).
         *
         * In order to deal with this scenario, we use an unsigned sort to treat all numbers are positive (in effect,
         * putting the negative numbers after the positive ones).
         */
        MoreArrays.unsignedSort(buffer);
    }

    private void insertIntoDense(int r){
        int p = r & 0x3F;
        int register;
        register = r>>>(Integer.SIZE-precision);
        if(p==0){
            /*
             * The value for p is stored in the register, we just have to pull it out
             */
            long lr = ((long)r)<<(Integer.SIZE+precision);
            /*
             * We know that lr!=0 on the important bits, because otherwise we wouldn't be in this
             * particular code branch
             */
            assert lr!=0: "Programmer error: lr should not be zero with a contained p value!";
            p = Long.numberOfLeadingZeros(lr)+1;
        }

        updateRegister(register,p);
    }

    private void convertToDense(){
        isSparse = false;
        denseRegisters = new byte[1<<precision];
        for(int i=0;i<sparseSize;i++){
            int sVal = sparseArray[i];
            if(sVal==0) continue;
            insertIntoDense(sVal);
        }
        //de-reference the sparse array to allow GC and save memory
        sparseArray=null;
    }

    /*
     * Inner class to maintain the logic for serialization/deserialization
     */
    static class EncoderDecoder implements Encoder<SparseHyperLogLog>{
        private final Hash64 hashFunction;

        public EncoderDecoder(Hash64 hashFunction){
            this.hashFunction=hashFunction;
        }

        @Override
        public void encode(SparseHyperLogLog item,DataOutput encoder) throws IOException{
            if(item.isSparse){
                item.mergeBuffer();
                if(item.isSparse){
                    encodeSparse(item,encoder);
                    return;
                }
            }
            encodeDense(item,encoder);
        }

        @Override
        public SparseHyperLogLog decode(DataInput input) throws IOException{
            if(input.readByte()==0x01){
                return decodeSparse(input);
            }else return decodeDense(input);
        }

        private SparseHyperLogLog decodeSparse(DataInput input) throws IOException{
            int precision=input.readInt();
            int size=input.readInt();
            int c=1;
            while(c<size){
                c<<=1;
            }
            int[] sparse=new int[c];
            for(int i=0;i<size;i++){
                sparse[i]=input.readInt();
            }
            DoubleFunction biasAdjuster=HyperLogLogBiasEstimators.biasEstimate(precision);
            return new SparseHyperLogLog(precision,hashFunction,biasAdjuster,size,sparse);
        }

        private SparseHyperLogLog decodeDense(DataInput input) throws IOException{
            int precision=input.readInt();
            int numRegisters=1<<precision;
            byte[] registers=new byte[numRegisters];
            input.readFully(registers);

            DoubleFunction biasAdjuster=HyperLogLogBiasEstimators.biasEstimate(precision);
            return new SparseHyperLogLog(precision,hashFunction,biasAdjuster,registers);
        }

        private void encodeSparse(SparseHyperLogLog item,DataOutput encoder) throws IOException{
            encoder.writeByte(0x01);
            encoder.writeInt(item.precision);
            encoder.writeInt(item.sparseSize);
            for(int i=0;i<item.sparseSize;i++){
                encoder.writeInt(item.sparseArray[i]);
            }
        }

        private void encodeDense(SparseHyperLogLog item,DataOutput encoder) throws IOException{
            encoder.writeByte(0x00);
            encoder.writeInt(item.precision);
            //we don't need the register length, because we can reconstruct it from the precision
            encoder.write(item.denseRegisters);
        }
    }
}
