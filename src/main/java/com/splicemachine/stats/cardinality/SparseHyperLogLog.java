package com.splicemachine.stats.cardinality;

import com.splicemachine.hash.Hash64;
import com.splicemachine.stats.DoubleFunction;

import java.util.Arrays;

/**
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
        this(size,hashFunction,HyperLogLogBiasEstimators.biasEstimate(size),3*(1<<size-6),3*(1<<size-6));
    }

    public SparseHyperLogLog(int precision,
                             Hash64 hashFunction,
                             DoubleFunction biasAdjuster,
                             int initialSparseSize,
                             int maxSparseSize){
        super(precision,hashFunction,biasAdjuster);
        if(initialSparseSize>maxSparseSize)
            initialSparseSize = maxSparseSize;

        int b =1;
        while(b<maxSparseSize){
            b<<=1;
        }
        int s = 1;
        while(s<initialSparseSize){
            s<<=1;
        }

//        this.sparseArray = new int[s];
        this.maxSparseSize = b;
        this.isSparse=true;
        if(s<4)
            this.buffer = new int[s];
        else
            this.buffer = new int[s>>2];
        this.bufferSize=0;
    }

    private SparseHyperLogLog(int precision,
                             Hash64 hashFunction,
                             DoubleFunction biasAdjuster,
                             int[] sparse,
                             int currSparseSize,
                             int maxSparseSize){
        super(precision,hashFunction,biasAdjuster);
        this.sparseArray =Arrays.copyOf(sparse,sparse.length);
        this.maxSparseSize = maxSparseSize;
        this.sparseSize = currSparseSize;
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
            return new SparseHyperLogLog(precision,hashFunction,biasAdjuster,sparseArray,sparseSize,maxSparseSize);
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
    protected int getRegister(int register){
        return denseRegisters[register];
    }

    @Override
    public long getEstimate(){
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
        int p = 0;
        if((r<<precision)==0){
            long w = hash<<precision;
            p=w==0?1:Long.numberOfLeadingZeros(w)+1;
            r |=p;
        }

        buffer[bufferSize]= r;
        bufferSize++;
        if(bufferSize==buffer.length)
            mergeBuffer();
//        int pos = sparseSize/2;
//
//        while(pos<sparseArray.length){
//            int sparseVal = sparseArray[pos];
//            if(sparseVal==0) {
//                sparseArray[pos] = r;
//                sparseSize++;
//                return;
//            }
//            int sparseReg = sparseVal>>>INT_SPARSE_SHIFT;
//            if(sparseReg>register){
//               //go right
//                pos=2*pos+2;
//            }else if(sparseReg<register){
//                pos = 2*pos+1;
//            }else{
//            }
//        }
//
//        if(sparseSize==maxSparseSize){
//            convertToDense();
//            insertIntoDense(r);
//        } else if(sparseArray.length<pos){
//            int s = 2*sparseArray.length;
//            while(s<pos+1)
//                s<<=1;
//            sparseArray = Arrays.copyOf(sparseArray,s);
//        }
//        sparseArray[pos] =r;
//        sparseSize++;
    }

    private void mergeBuffer(){
        if(sparseSize+bufferSize>=maxSparseSize){
            convertToDense();
            for(int i=0;i<bufferSize;i++){
                insertIntoDense(buffer[i]);
            }
        } else if(sparseSize==0){
            //we can avoid an extra data copy on the first merge()
            Arrays.sort(buffer);
            sparseArray = buffer;
            buffer = new int[buffer.length];
        }else if(bufferSize==0){
            //there's nothing to do
            return;
        } else{
            Arrays.sort(buffer);
            int bPos = 0;
            int sPos = 0;
            int dPos = 0;
            int[] dest = new int[sparseSize+bufferSize];
            int buff = buffer[bPos];
            int bReg = buff>>>(Integer.SIZE-precision);
            int sp = sparseArray[sPos];
            int sReg = sp>>>(Integer.SIZE-precision);
            OUTER_LOOP: while(dPos<dest.length){
                while(bReg<sReg){
                    int bn;
                    for(bn = bPos+1;bn<bufferSize;bn++){
                        int b = buffer[bn];

                    }
                }
            }
            sparseArray = dest;
            sparseSize = dPos;
        }
        bufferSize=0;
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
        sparseArray = null;
    }
}
