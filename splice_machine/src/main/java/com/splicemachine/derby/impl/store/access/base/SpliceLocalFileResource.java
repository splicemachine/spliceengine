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

package com.splicemachine.derby.impl.store.access.base;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.SyncFailedException;

import com.splicemachine.db.io.StorageFactory;

/**
 * A FileResource implementation for writing and reading JAR files to and from the local file system.
 *
 * @author dwinters
 */

public class SpliceLocalFileResource extends SpliceBaseFileResource {

	public SpliceLocalFileResource(StorageFactory storageFactory) {
		super(storageFactory);
	}

	@Override
	protected void syncOutputStream(OutputStream os) throws IOException {
		if(os instanceof FileOutputStream)
			((FileOutputStream) os).getFD().sync();
	}
}
