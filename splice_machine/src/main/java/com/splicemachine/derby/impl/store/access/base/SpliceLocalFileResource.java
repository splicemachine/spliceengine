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

package com.splicemachine.derby.impl.store.access.base;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

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
