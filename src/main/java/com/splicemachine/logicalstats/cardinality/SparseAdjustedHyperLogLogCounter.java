package com.splicemachine.logicalstats.cardinality;

import com.google.common.primitives.Ints;
import com.splicemachine.logicalstats.DoubleFunction;
import com.splicemachine.utils.hash.Hash64;

import java.util.Arrays;

/**
 * A Sparsely-stored, bias-adjusted HyperLogLog Counter.
 *
 * <p>This implementation takes advantage of small cardinalities to both
 * be more memory efficient and potentially to increase the overall accuracy of
 * the estimation.</p>
 *
 * <p>When the observed cardinality is very small, then most registers are empty,
 * resulting in a considerable waste of space, particularly for higher accuracy. To
 * avoid this situation, this implementation begins by storing data in a sparse integer
 * array, where the first few bits of the integer stores the index of the register,
 * and the remaining bits store the value. Of course, if the number of non-empty
 * registers grows beyond a certain point, the sparse representation becomes less
 * memory-efficient than a dense one, so the implementation will automatically switch
 * to a dense implementation after that point.</p>
 *
 * <p>Our sparse encoding is as follows. Let {@code p} be the precision desired. Then,
 * we know that {@code idx} is the register to which the value belongs. Set {@code p(w)}
 * to be the leftmost 1-bit in the hashed value.</p>
 *
 * <p>We know from algorithm analysis that we can store {@code p(w)} in 6 bits, but we require
 * {@code p} bits to store {@code idx}. Thus, we need at least {@code p+6} bits to store the
 * sparse representation.</p>
 *
 * <p>Since {@code p+6} is not likely to be a full word of any size, we will end up wasting at least
 * some bits. Why not waste them storing real data? Instead of storing p bits for the index, store
 * {@code idx' = wordSize-6} bits of the hash. This results in more registers, and thus better accuracy,
 * without using any storage space that we weren't using already.</p>
 *
 * <p>Furthermore, we now have a situation where {@code p(w)} may already stored in the {@code idx'} value
 * (if there is a 1-bit contains somewhere in the least significant {wordSize-p}). In that case, we don't
 * need to store the value at all, we know it already. Thus, if we use the least-significant bit to indicate
 * whether or not {@code p(w)} is present, we can save ourselves the necessity of storing the same value every time.
 * This takes one bit, so we store one less than wordSize. Thus, we end up with a format of
 *
 * idx' | p(w) | 1
 *
 * if we needed to store {@code p(w)}. Otherwise, we end up with
 *
 * idx' | 0
 *
 * Thus, for an int, we have 25-bits for {@code idx'}, 1 bit for the indicator field, and 6 bits for {@code p(w)}.</p>
 *
 * <p>When we go to add an entry, we first compose an int with the value. If the least-significant bit is 0, then
 * all we need to do is ensure that the register is present, since all values to that bucket will have the same
 * p(w). Otherwise, we will need to do a comparison on the 6-bits {@code p(w)} value to keep the maximum.</p>
 *
 * <p>Finally, we keep this array sorted, which allows us to efficiently convert between the sparse and
 * dense representations when necessary. When we go to convert to a dense representation, however, we will
 * down-convert the precision to be whatever the user calls for (so if the user asks for precision 10, we will
 * use a dense representation with precision 10, not 25), so some precision is lost at that point. However, no
 * precision is lost if the sparse representation is kept all the way through.</p>
 *
 * <p>In truth, one notices that, if the sparse representation is maintained throughout all updates and an estimate
 * is asked for, then the cardinality is low enough that fewer than {@code 3*2^(precision-6)} registers were filled,
 * which is generally well below the empirical threshold at which it's more accurate to use Linear counting anyway.
 * Thus, we can immediately perform linear counting without needing to perform the hyperloglog estimate at all.</p>
 *
 * <p>Updates to a sparse structure are relatively expensive, however. To this end, this implementation buffers
 * results up to a threshold before flushing those changes to the sparse structure in a single pass. By default, this
 * buffer threshold is set to be 25% of the maximum sparse register size (25% of {@code 3*2^(precision-6)}</p
 *
 * @author Scott Fines
 * Date: 1/2/14
 */
public class SparseAdjustedHyperLogLogCounter extends BaseBiasAdjustedHyperLogLogCounter {
		private static final int PRECISION_BITS = 25;
		private static final int REGISTER_SHIFT = Integer.SIZE - PRECISION_BITS;
		private boolean isSparse = true;

		/**
		 * data held in these registers when the mode is dense. This occurs when
		 * 3*2^(p-6) < sparse.length (e.g. when the memory consumed by the sparse representation
		 * is more than what would be consumed by the dense).
		 */
		private byte[] denseRegisters;

		private int[] sparse;

		/*
		 * Because inserting into the sparse list is expensive (we have to keep it sorted), when you go
		 * to update a register, you really just add it to this buffer. When the buffer fills up, the sparse
		 * array is updated in bulk, saving repeated operations against the same buckets. This buffer is generally
		 * set to a small value, but allowed to grow to 25% of the maximum sparse size (although the maximum is adjustable).
		 */
		private int[] buffer;
		private int bufferPos=0;

		private final int maxSparseSize;
		private int maxBufferSize;
		private int sparseSize = 0;

		public SparseAdjustedHyperLogLogCounter(int precision, Hash64 hashFunction) {
				this(precision, 2,-1,hashFunction);
		}

		public SparseAdjustedHyperLogLogCounter(int precision, Hash64 hashFunction,
																						DoubleFunction biasAdjuster) {
				this(precision, 2, -1,hashFunction, biasAdjuster);
		}

		public SparseAdjustedHyperLogLogCounter(int precision, int initialSize, int maxBufferSize,
																						Hash64 hashFunction) {
				super(precision, hashFunction);

				maxSparseSize = 3*(1<<(precision-6));
				initialize(initialSize, maxBufferSize);
		}

		public SparseAdjustedHyperLogLogCounter(int precision, int initialSize, int maxBufferSize,
																						Hash64 hashFunction,
																						DoubleFunction biasAdjuster) {
				super(precision, hashFunction, biasAdjuster);
				maxSparseSize = 3*(1<<(precision-6));

				initialize(initialSize, maxBufferSize);
		}

		@Override
		protected void doUpdate(long hash) {
				int p;
				int register;
				if(isSparse){
						register = (int)(hash>>>(Long.SIZE-PRECISION_BITS));
						p = register<< precision;
						if(p==0){
								long w = hash<<PRECISION_BITS;
								p = w==0l?1: (Long.numberOfLeadingZeros(w)+1);
						}else{
								/*
								 * We know that the proper value for p is contained
								 * in the register, so we don't need to worry about it.
								 */
								p = 0;
						}
						int val = register<<REGISTER_SHIFT;
						val |=(p<<1);
						val |= p==0? 0: 1;
						buffer[bufferPos] = val;
						bufferPos++;
						if(bufferPos>=buffer.length){
								resizeOrMergeBuffer();
						}
				}else{
						super.update(hash);
				}
		}

		@Override
		protected void updateRegister(int register, int value) {
				byte curr = denseRegisters[register];
				if(curr>=value) return;
				denseRegisters[register] = (byte)(value & 0xff);
		}

		@Override
		public long getEstimate() {
				if(isSparse){
						if(bufferPos>0)
								merge();
						if(isSparse){
								//count zero registers and use linear counting for our estimate
								int mP = (1<<PRECISION_BITS);
								int numZeroRegisters = mP-sparseSize; //the number of missing entries is the number of zero registers
								return (long)(mP*Math.log((double)mP/numZeroRegisters));
						}else
								return super.getEstimate();
				}else
						return super.getEstimate();
		}

		@Override
		protected int getRegister(int register) {
				return denseRegisters[register];
		}

		/***********************************************************************************************************/
		/*private helper functions*/
		private void initialize(int initialSize, int maxBufferSize) {
				if(maxBufferSize<0)
						maxBufferSize = maxSparseSize/4; //default to 25% of the max sparseSize
				if(initialSize>=maxSparseSize){
						isSparse=false;
						this.denseRegisters = new byte[numRegisters];
				}else{
						this.sparse = new int[initialSize];
						this.maxBufferSize = maxBufferSize;
						if(initialSize<maxBufferSize)
								this.buffer = new int[initialSize];
						else
								this.buffer = new int[maxBufferSize];
				}
		}

		private void resizeOrMergeBuffer() {
				if(buffer.length==maxBufferSize){
						merge();
				}else{
						int[] newBuffer = new int[Math.min(buffer.length*3/2,maxBufferSize)];
						System.arraycopy(buffer,0,newBuffer,0,buffer.length);
						buffer = newBuffer;
				}
		}

		private void merge() {
				/*
				 * Merge the buffer into the sparse array.
				 *
				 * This is done in 4 stages:
				 *
				 * 1. sort buffer
				 * 2. insert into sparse array in sorted order
				 * 3. if sparse array size is too large, convert to dense and empty sparse array
				 * 4. empty buffer.
				 */
				Arrays.sort(buffer, 0, bufferPos);

				int sparsePos=0;
				int bufferPointer=0;
				while(isSparse && bufferPointer<bufferPos){
						int bufferEntry = buffer[bufferPointer];
						int bufferRegister = bufferEntry>>>REGISTER_SHIFT;
						boolean containsP = (bufferEntry & 1) !=0;

						if(sparsePos>=sparseSize){
								insertIntoSparse(sparsePos,bufferEntry);
						}else{
								int sparseEntry = sparse[sparsePos];
								int sparseRegister = sparseEntry>>>REGISTER_SHIFT;
								int compare = Ints.compare(bufferRegister, sparseRegister);
								while(sparseEntry!=0 && sparsePos<sparseSize && compare>0){
										sparsePos++;
										if(sparsePos==sparseSize)
												break;

										sparseEntry = sparse[sparsePos];
										sparseRegister = sparseEntry>>>REGISTER_SHIFT;
										compare = Ints.compare(bufferRegister,sparseRegister);
								}

								if(compare<0 || sparsePos>=sparseSize || sparseEntry==0){
										insertIntoSparse(sparsePos, bufferEntry);
								}else if(!containsP){
										//replace sparse with the buffer entry if the p(w) is higher
										int bP = (bufferEntry & 0x7E);
										int sP = (sparseEntry & 0x7E);
										if(bP >sP){
												sparse[sparsePos] = bufferEntry;
										}
								}
						}
						bufferPointer++;
				}
				for(int i=bufferPointer;i<bufferPos;i++){
						int bufferEntry = buffer[i];
						if(isSparse){
								insertIntoSparse(sparsePos, bufferEntry);
								sparsePos++;
						}else
								insertIntoDense(bufferEntry);
				}
				if(!isSparse){
						sparse=null;
						buffer = null;
				}

				this.bufferPos=0;
		}

		private void insertIntoDense(int bufferEntry) {
				int register = bufferEntry>>> REGISTER_SHIFT;
				int p;
				if((bufferEntry & 1)!=0){
						p = bufferEntry & 0x7E;
				}else{
						int w = register<< precision;
						p = Integer.numberOfLeadingZeros(w)+1;
				}
				register>>>=(PRECISION_BITS- precision);
				int current = denseRegisters[register];
				if(current<p)
						denseRegisters[register] = (byte)(p&0xff);
		}

		private void insertIntoSparse(int pos, int entry) {
				if(sparseSize>=maxSparseSize){
						convertToDense();
						insertIntoDense(entry);
						return;
				}
				sparseSize++;

				if(sparseSize>sparse.length){
						//resize sparse array
						int[] newSparse = new int[Math.min(sparse.length*2,maxSparseSize)];
						System.arraycopy(sparse,0,newSparse,0,Math.min(sparse.length,pos+1));
						sparse = newSparse;
				}
				if(sparseSize>pos+1)
						System.arraycopy(sparse,pos,sparse,pos+1,sparseSize-1-pos);
				sparse[pos] = entry;
		}

		private void convertToDense() {
			/*
			 * Convert the sparse representation into a dense one. This is done
			 * by iterating through, finding the proper registers and p values, and inserting them
			 */
				isSparse=false;
				denseRegisters = new byte[numRegisters];
				for(int i=0;i<sparseSize;i++){
						insertIntoDense(sparse[i]);
				}
		}

}
