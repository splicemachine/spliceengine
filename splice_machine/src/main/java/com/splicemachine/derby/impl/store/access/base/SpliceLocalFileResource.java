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
		((FileOutputStream) os).getFD().sync();
	}
}
