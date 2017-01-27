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
import com.splicemachine.kvpair.KVPair;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 11/15/13
 */
public class PairDecoder{
    private final KeyDecoder keyDecoder;
    private final KeyHashDecoder rowDecoder;
    private final ExecRow templateRow;

    public PairDecoder(KeyDecoder keyDecoder,
                       KeyHashDecoder rowDecoder,
                       ExecRow templateRow){
        this.keyDecoder=keyDecoder;
        this.rowDecoder=rowDecoder;
        this.templateRow=templateRow;
    }

    public ExecRow decode(KVPair kvPair) throws StandardException{
        templateRow.resetRowArray();
        keyDecoder.decode(kvPair.getRowKey(),0,kvPair.getRowKey().length,templateRow);
        rowDecoder.set(kvPair.getValue(),0,kvPair.getValue().length);
        rowDecoder.decode(templateRow);
        return templateRow;
    }

    public ExecRow decode(byte[] key, byte[] value) throws StandardException{
        templateRow.resetRowArray();
        keyDecoder.decode(key,0,key.length,templateRow);
        rowDecoder.set(value,0,value.length);
        rowDecoder.decode(templateRow);
        return templateRow;

    }


		/*
         *
		 *  < a | b |c >
		 *    1 | 2 | 3
		 *
		 *  sort (a) -->
		 *  Row Key: 1
		 *  Row Data: 2 | 3
		 *
		 *  group (a,b) ->
		 *  Row Key: a | b
		 *  Row Data: aggregate(c)
		 */

    public int getKeyPrefixOffset(){
        return keyDecoder.getPrefixOffset();
    }

    public ExecRow getTemplate(){
        return templateRow;
    }

    @Override
    public String toString(){
        return String.format("PairDecoder { keyDecoder=%s rowDecoder=%s, templateRow=%s}",keyDecoder,rowDecoder,templateRow);
    }

    public void close() throws IOException{
        try{
            if(keyDecoder!=null)
                keyDecoder.close();
        }finally{
            if(rowDecoder!=null)
                rowDecoder.close();
        }
    }

}
