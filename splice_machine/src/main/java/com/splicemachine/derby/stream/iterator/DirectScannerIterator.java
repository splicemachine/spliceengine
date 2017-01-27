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

package com.splicemachine.derby.stream.iterator;


import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.kvpair.KVPair;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by jyuan on 10/16/15.
 */
@NotThreadSafe
public class DirectScannerIterator implements Iterable<KVPair>, Iterator<KVPair>, Closeable {
    private DirectScanner directScanner;
    private KVPair value;
    private boolean populated=false;

    public DirectScannerIterator(DirectScanner hTableBuilder) throws IOException, StandardException{
        this.directScanner= hTableBuilder;
        directScanner.open();
    }

    @Override
    public Iterator<KVPair> iterator() {
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
    public KVPair next() {
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
