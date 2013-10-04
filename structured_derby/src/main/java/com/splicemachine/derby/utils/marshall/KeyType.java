package com.splicemachine.derby.utils.marshall;

import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.tools.ThreadLocalRandom;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Scott Fines
 *         Created on: 6/12/13
 */
public enum KeyType implements KeyMarshall{
    /**
     * Prepends a fixed (non-unique) Prefix to the row key,
     * but does nothing for a postfix.
     */
    FIXED_PREFIX {
        @Override
        public void encodeKey(DataValueDescriptor[] fields,
                              int[] keyColumns,
                              boolean[] sortOrder,
                              byte[] keyPostfix,
                              MultiFieldEncoder keyEncoder) throws StandardException {
            //the prefix will have already been set in the constructor,
            //so we just delegate to BARE
            BARE.encodeKey(fields,keyColumns,sortOrder,keyPostfix,keyEncoder);
        }

        @Override
        public void decode(DataValueDescriptor[] fields,
                           int[] reversedKeyColumns,
                           boolean[] sortOrder,
                           MultiFieldDecoder rowDecoder) throws StandardException {
            //throw away the first bytes--treat them as a string
            rowDecoder.seek(9);
            BARE.decode(fields,reversedKeyColumns,sortOrder,rowDecoder);
        }

        @Override
        public int getFieldCount(int[] keyColumns) {
            return BARE.getFieldCount(keyColumns)+1;
        }
    },
    /**
     * Appends a postfix to the row key, but does nothing with a prefix.
     */
    FIXED_POSTFIX {
        @Override
        public void encodeKey(DataValueDescriptor[] fields,
                              int[] keyColumns,
                              boolean[] sortOrder,
                              byte[] keyPostfix,
                              MultiFieldEncoder keyEncoder) throws StandardException {
            //delegate to BARE, then append the postfix
            BARE.encodeKey(fields,keyColumns,sortOrder,keyPostfix,keyEncoder);
            /*
             * The postfix will never be directly decoded (although it might be used for
             * correctness checking), so it's safe to setRawBytes() directly.
             */
            keyEncoder.setRawBytes(keyPostfix);
        }

        @Override
        public void decode(DataValueDescriptor[] fields,
                           int[] reversedKeyColumns,
                           boolean[] sortOrder,
                           MultiFieldDecoder rowDecoder) throws StandardException {
            BARE.decode(fields,reversedKeyColumns,sortOrder,rowDecoder);
        }

        @Override
        public int getFieldCount(int[] keyColumns) {
            return BARE.getFieldCount(keyColumns)+1;
        }
    },
    UNIQUE_POSTFIX {
        private final AtomicLong counter = new AtomicLong(0l);
        @Override
        public void encodeKey(DataValueDescriptor[] fields, int[] keyColumns, boolean[] sortOrder, byte[] keyPostfix, MultiFieldEncoder keyEncoder) throws StandardException {
            BARE.encodeKey(fields,keyColumns,sortOrder,keyPostfix,keyEncoder);
            //encode random bits at the end of the postfix for uniqueness --TODO -sf- make this more compact, and make SuccessFilter deal with it
            keyEncoder.encodeNext(counter.incrementAndGet());
            /*
             * encode the postfix in place
             *
             * The postfix will never be directly decoded (although it might be used for
             * correctness checking), so it's safe to setRawBytes() directly.
             */
            keyEncoder.setRawBytes(keyPostfix);
        }

        @Override
        public void decode(DataValueDescriptor[] fields, int[] reversedKeyColumns,
                           boolean[] sortOrder,MultiFieldDecoder rowDecoder) throws StandardException {
            BARE.decode(fields,reversedKeyColumns,sortOrder,rowDecoder);
        }

        @Override
        public int getFieldCount(int[] keyColumns) {
            return BARE.getFieldCount(keyColumns)+2;
        }
    },
    /**
     * Prepends a prefix, and appends a postfix to the row key
     */
    FIXED_PREFIX_AND_POSTFIX {
        @Override
        public void encodeKey(DataValueDescriptor[] fields,
                              int[] keyColumns,
                              boolean[] sortOrder,
                              byte[] keyPostfix,
                              MultiFieldEncoder keyEncoder) throws StandardException {
            //the prefix is set in the constructor, so this functions
            //the same as POSTFIX_ONLY
            FIXED_POSTFIX.encodeKey(fields, keyColumns, sortOrder, keyPostfix, keyEncoder);
        }

        @Override
        public void decode(DataValueDescriptor[] fields,
                           int[] reversedKeyColumns,
                           boolean[] sortOrder,
                           MultiFieldDecoder rowDecoder) throws StandardException {
            FIXED_PREFIX.decode(fields, reversedKeyColumns, sortOrder,rowDecoder);
        }

        @Override
        public int getFieldCount(int[] keyColumns) {
            return FIXED_POSTFIX.getFieldCount(keyColumns)+1;
        }
    },
    FIXED_PREFIX_UNIQUE_POSTFIX {
        @Override
        public void encodeKey(DataValueDescriptor[] fields, int[] keyColumns, boolean[] sortOrder, byte[] keyPostfix, MultiFieldEncoder keyEncoder) throws StandardException {
            UNIQUE_POSTFIX.encodeKey(fields, keyColumns, sortOrder, keyPostfix, keyEncoder);
        }

        @Override
        public void decode(DataValueDescriptor[] fields, int[] reversedKeyColumns, boolean[] sortOrder,MultiFieldDecoder rowDecoder) throws StandardException {
            FIXED_PREFIX.decode(fields, reversedKeyColumns, sortOrder,rowDecoder);
        }

        @Override
        public int getFieldCount(int[] keyColumns) {
            return UNIQUE_POSTFIX.getFieldCount(keyColumns)+1;
        }
    },
    PREFIX_ONLY {
        @Override
        public void encodeKey(DataValueDescriptor[] fields, int[] keyColumns, boolean[] sortOrder, byte[] keyPostfix, MultiFieldEncoder keyEncoder) throws StandardException {
            //no-op, prefix is set in the constructor
        }

        @Override
        public void decode(DataValueDescriptor[] fields, int[] reversedKeyColumns, boolean[] sortOrder,MultiFieldDecoder rowDecoder) throws StandardException {
            //no-op, no fields are present in the key
        }

        @Override
        public int getFieldCount(int[] keyColumns) {
            return 1;
        }
    },
    PREFIX_FIXED_POSTFIX_ONLY{
        @Override
        public void encodeKey(DataValueDescriptor[] fields, int[] keyColumns, boolean[] sortOrder, byte[] keyPostfix, MultiFieldEncoder keyEncoder) throws StandardException {
            /*
             * The postfix will never be directly decoded (although it might be used for
             * correctness checking), so it's safe to setRawBytes() directly.
             */
            keyEncoder.setRawBytes(keyPostfix);
        }

        @Override
        public void decode(DataValueDescriptor[] fields, int[] reversedKeyColumns, boolean[] sortOrder,MultiFieldDecoder rowDecoder) throws StandardException {
            //no-op, no fields are present in the key
        }

        @Override
        public int getFieldCount(int[] keyColumns) {
            return 2;
        }
    },
    PREFIX_UNIQUE_POSTFIX_ONLY{
        @Override
        public void encodeKey(DataValueDescriptor[] fields, int[] keyColumns, boolean[] sortOrder, byte[] keyPostfix, MultiFieldEncoder keyEncoder) throws StandardException {
            /*
             * Add a uniqueness field
             *
             * It is safe to call setRawBytes() here, since this field will never be used for
             * decoding.
             */
            keyEncoder.setRawBytes(SpliceUtils.getUniqueKey());
            /*
             * The postfix will never be directly decoded (although it might be used for
             * correctness checking), so it's safe to setRawBytes() directly.
             */
            keyEncoder.setRawBytes(keyPostfix);
        }

        @Override
        public void decode(DataValueDescriptor[] fields, int[] reversedKeyColumns,boolean[] sortOrder, MultiFieldDecoder rowDecoder) throws StandardException {
            //no-op, no fields are present in the key
        }

        @Override
        public int getFieldCount(int[] keyColumns) {
            return 3;
        }
    },
    /**
     * Constructs a key from a 4-byte random salt and a UUID
     */
    SALTED {
        @Override
        public void encodeKey(DataValueDescriptor[] fields,
                              int[] keyColumns,
                              boolean[] sortOrder,
                              byte[] keyPostfix,
                              MultiFieldEncoder keyEncoder) {
            /*
             * salted ignores row, keycolumn,sortOrder because it generates
             * a row key randomly.
             *
             * Since the key will never be decoded, it's safe to just shove
             * the bytes in directly
             */
            keyEncoder.setRawBytes(SpliceUtils.getUniqueKey());
        }

        @Override
        public void decode(DataValueDescriptor[] fields, int[] reversedKeyColumns,
                           boolean[] sortOrder,MultiFieldDecoder rowDecoder) throws StandardException {
            //no-op--no fields are present in the key
        }

        @Override
        public int getFieldCount(int[] keyColumns) {
            return 2;
        }
    },
    /**
     * No prefix or Postfix is to be used
     */
    BARE {
        @Override
        public void encodeKey(DataValueDescriptor[] fields,
                              int[] keyColumns,
                              boolean[] sortOrder,
                              byte[] keyPostfix,
                              MultiFieldEncoder keyEncoder) throws StandardException {
            if(sortOrder!=null){
            	for (int i=0;i<keyColumns.length;i++) {
                    boolean desc = !sortOrder[i];
                    DataValueDescriptor dvd = fields[keyColumns[i]];
                    if(dvd.isNull())
                        DerbyBytesUtil.encodeTypedEmpty(keyEncoder,dvd,desc,true);
                    else
                        DerbyBytesUtil.encodeInto(keyEncoder, dvd, desc);
                }
            }else{
                for(int key:keyColumns){
                    DataValueDescriptor dvd = fields[key];
                    if(dvd.isNull())
                        DerbyBytesUtil.encodeTypedEmpty(keyEncoder,dvd,false,true);
                    else
                        DerbyBytesUtil.encodeInto(keyEncoder, dvd,false);
                }
            }
        }
        
        @Override
        public void decode(DataValueDescriptor[] fields, int[] reversedKeyColumns,
                boolean[] sortOrder,MultiFieldDecoder rowDecoder) throws StandardException {
			 if(sortOrder!=null){
			 	for (int i = 0; i<reversedKeyColumns.length; i++) {
			         boolean desc = !sortOrder[i];
			         if (reversedKeyColumns[i] != -1) {
			              if (DerbyBytesUtil.isNextFieldNull(rowDecoder,fields[reversedKeyColumns[i]])) {
			             	 fields[reversedKeyColumns[i]].setToNull();
			                  DerbyBytesUtil.skip(rowDecoder,fields[reversedKeyColumns[i]]);
			              }else
			                  DerbyBytesUtil.decodeInto(rowDecoder, fields[reversedKeyColumns[i]],desc);
			         }
			     }
			 }else{
			     for (int rowSpot : reversedKeyColumns) {
			         if (rowSpot != -1) {
			             if (DerbyBytesUtil.isNextFieldNull(rowDecoder,fields[rowSpot])) {
			            	 fields[rowSpot].setToNull();
			                 DerbyBytesUtil.skip(rowDecoder,fields[rowSpot]);
			             }else
			                 DerbyBytesUtil.decodeInto(rowDecoder, fields[rowSpot]);
			         }
			     }
			 }
}

        @Override
        public int getFieldCount(int[] keyColumns) {
            if(keyColumns==null)
                return 0;
            return keyColumns.length;
        }
    }
}
