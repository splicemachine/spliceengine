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

package com.splicemachine.derby.impl.store.access;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * @author Scott Fines
 *         Date: 1/11/16
 */
public class FileResourceFactoryService{
    public static FileResourceFactory loadFileResourceFactory(){
        ServiceLoader<FileResourceFactory> loader = ServiceLoader.load(FileResourceFactory.class);
        Iterator<FileResourceFactory> iter = loader.iterator();
        if(!iter.hasNext())
            throw new IllegalStateException("No FileResourceFactory found!");
        FileResourceFactory frf = iter.next();
        if(iter.hasNext())
            throw new IllegalStateException("More than one FileResourceFactory is found!");
        return frf;
    }
}
