/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.utils.marshall;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 11/15/13
 */
public class KeyEncoder implements Closeable{
    private final HashPrefix prefix;
    private final DataHash hash;
    private final KeyPostfix postfix;

    public KeyEncoder(HashPrefix prefix,DataHash hash,KeyPostfix postfix){
        this.prefix=prefix;
        this.hash=hash;
        this.postfix=postfix;
    }

    @Override
    public void close() throws IOException{
        prefix.close();
        hash.close();
        postfix.close();
    }

    public byte[] getKey(ExecRow row) throws StandardException, IOException{
        hash.setRow(row);
        byte[] hashBytes=hash.encode();

        int prefixLength=prefix.getPrefixLength();
        int totalLength=prefixLength+hashBytes.length;
        int postfixOffset=prefixLength+hashBytes.length;
        int postfixLength=postfix.getPostfixLength(hashBytes);
        if(postfixLength>0){
            totalLength++;
            postfixOffset++;
        }
        totalLength+=postfixLength;
        byte[] finalRowKey=new byte[totalLength];
        prefix.encode(finalRowKey,0,hashBytes);
        if(hashBytes.length>0){
            System.arraycopy(hashBytes,0,finalRowKey,prefixLength,hashBytes.length);
        }
        if(postfixLength>0){
            finalRowKey[prefixLength+hashBytes.length]=0x00;
        }
        postfix.encodeInto(finalRowKey,postfixOffset,hashBytes);

        return finalRowKey;
    }

    public KeyDecoder getDecoder(){
        return new KeyDecoder(hash.getDecoder(),prefix.getPrefixLength());
    }

    public static KeyEncoder bare(int[] groupColumns,boolean[] groupSortOrder,DescriptorSerializer[] serializers){
        return new KeyEncoder(NoOpPrefix.INSTANCE,BareKeyHash.encoder(groupColumns,groupSortOrder,serializers),NoOpPostfix.INSTANCE);
    }
}
