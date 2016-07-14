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

package com.splicemachine.storage;

/**
 * A Predicate that compares Char values.
 * <p/>
 * with Char data types, the insertion will have additional whitespace (potentially),
 * but the scan is not required (and often doesn't) have the same whitespace. In order to
 * not break the case when users specifically include whitespace, we don't want to trim
 * during insertion. So instead we use this comparison (instead of the ValuePredicate)
 * which will ignore the whitespace byte during comparison.
 *
 * @author Scott Fines
 *         Date: 3/4/14
 */
public class CharValuePredicate extends ValuePredicate{

    public CharValuePredicate(CompareOp compareOp,
                              int column,
                              byte[] compareValue,
                              boolean removeNullEntries,
                              boolean desc){
        super(compareOp,column,compareValue,removeNullEntries,desc);
    }

    @Override
    protected int doComparison(byte[] data,int offset,int length){
        if(length<=compareValue.length){
                        /*
						 * We are looking at data that cannot contain extra
						 * whitespace, so just delegate to our parent behavior
						 */
            return super.doComparison(data,offset,length);
        }

        //find any whitespace in the data
        int pos=offset+length-1;
        //go backwards in steps of 8
        while(pos>=offset && (pos-offset>=compareValue.length) && data[pos]==34){ //34 is the encoded value of a whitespace terminator
            pos--;
        }

        int adjustedWhitespaceLength=pos-offset+1;

        return super.doComparison(data,offset,adjustedWhitespaceLength);
    }

    @Override
    public byte[] toBytes(){
        return super.toBytes();
    }

    @Override
    protected PredicateType getType(){
        return PredicateType.CHAR_VALUE;
    }
}
