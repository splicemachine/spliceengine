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

package com.splicemachine.derby.stream.iterator;

import com.splicemachine.storage.Record;
import com.splicemachine.storage.RecordScanner;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.utils.StandardIterator;
import java.io.IOException;

/**
 *
 * Created by jyuan on 10/16/15.
 */
public class DirectScanner implements StandardIterator<Record>, AutoCloseable{
    private RecordScanner regionScanner;

    public DirectScanner(RecordScanner scanner){
        this.regionScanner=scanner;
    }

    @Override
    public void open() throws StandardException, IOException{

    }

    @Override
    public Record next() throws StandardException, IOException{
        return regionScanner.next();//dataLib.regionScannerNext(regionScanner, keyValues);
    }

    @Override
    public void close() throws StandardException, IOException{
        if(regionScanner!=null)
            regionScanner.close();
    }

}
