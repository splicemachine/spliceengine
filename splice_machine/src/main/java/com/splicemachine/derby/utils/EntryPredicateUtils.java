/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.utils;

import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.Predicate;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

/**
 * Created with IntelliJ IDEA.
 * User: jyuan
 * Date: 3/7/14
 * Time: 1:57 PM
 * To change this template use File | Settings | File Templates.
 */
public class EntryPredicateUtils {

    public static boolean qualify(EntryPredicateFilter predicateFilter, byte[] data, int offset, int length,
                                  DataValueDescriptor[] kdvds, int [] columnOrdering,
                                  MultiFieldDecoder keyDecoder) throws StandardException {
        int ibuffer = predicateFilter.getValuePredicates().size();
        if (ibuffer == 0)
            return true;
        keyDecoder.set(data,offset,length);
        Object[] buffer = predicateFilter.getValuePredicates().buffer;

        for (int i = 0; i < kdvds.length; ++i) {
            if (kdvds[i] == null) continue;
            int nOffset = keyDecoder.offset();
            DerbyBytesUtil.skip(keyDecoder,kdvds[i]);
            int size = keyDecoder.offset()-nOffset-1;
            for (int j =0; j<ibuffer; j++) {
                if(((Predicate)buffer[j]).applies(columnOrdering[i]) &&
                        !((Predicate)buffer[j]).match(columnOrdering[i],data,nOffset,size))
                    return false;
            }
        }

        return true;
    }
}
