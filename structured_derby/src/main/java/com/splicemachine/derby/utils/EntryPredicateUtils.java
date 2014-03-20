package com.splicemachine.derby.utils;

import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.Predicate;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;

/**
 * Created with IntelliJ IDEA.
 * User: jyuan
 * Date: 3/7/14
 * Time: 1:57 PM
 * To change this template use File | Settings | File Templates.
 */
public class EntryPredicateUtils {

    public static boolean qualify(EntryPredicateFilter predicateFilter, byte[] data, int initOffset, int length,
                                  DataValueDescriptor[] kdvds, int [] columnOrdering,
                                  MultiFieldDecoder keyDecoder) throws StandardException {
        int ibuffer = predicateFilter.getValuePredicates().size();
        if (ibuffer == 0)
            return true;
        keyDecoder.set(data,initOffset,length);
        Object[] buffer = predicateFilter.getValuePredicates().buffer;

        for (int i = 0; i < kdvds.length; ++i) {
            if (kdvds[i] == null) continue;
            int offset = keyDecoder.offset();
            DerbyBytesUtil.skip(keyDecoder,kdvds[i]);
            int size = keyDecoder.offset()-offset-1;
            for (int j =0; j<ibuffer; j++) {
                if(((Predicate)buffer[j]).applies(columnOrdering[i]) &&
                        !((Predicate)buffer[j]).match(columnOrdering[i],data,offset,size))
                    return false;
            }
        }

        return true;
    }
}
