/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.si.impl;

import com.splicemachine.si.api.data.ReadOnlyModificationException;
import org.apache.hadoop.hbase.DoNotRetryIOException;

/**
 * Indicates that an attempt was made to perform a write
 * with a read-only transaction
 *
 * @author Scott Fines
 *         Date: 6/24/14
 */
public class HReadOnlyModificationException extends DoNotRetryIOException implements ReadOnlyModificationException{
    public HReadOnlyModificationException(){
    }

    public HReadOnlyModificationException(String message){
        super(message);
    }

    public HReadOnlyModificationException(String message,Throwable cause){
        super(message,cause);
    }
}

