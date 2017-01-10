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


import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.storage.Record;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by jyuan on 10/16/15.
 */
@NotThreadSafe
public class DirectScannerIterator implements Iterable<Record>, Iterator<Record>, Closeable {
    private DirectScanner directScanner;
    private Record value;
    private boolean populated=false;

    public DirectScannerIterator(DirectScanner hTableBuilder) throws IOException, StandardException{
        this.directScanner= hTableBuilder;
        directScanner.open();
    }

    @Override
    public Iterator<Record> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        if(populated) return value!=null;
        populated=true;
        try {
            value = directScanner.next();
            if (value == null) {
                close();
                return false;
            }else
                return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Record next() {
        if(!hasNext()) throw new NoSuchElementException();
        populated=false;
        return value;
    }

    @Override
    public void remove() {
        throw new RuntimeException("Not Implemented");
    }

    @Override
    public void close() throws IOException {
        if (directScanner!= null) {
            try {
                directScanner.close();
            } catch (StandardException se) {
                throw new IOException(se);
            }
        }
    }
}
