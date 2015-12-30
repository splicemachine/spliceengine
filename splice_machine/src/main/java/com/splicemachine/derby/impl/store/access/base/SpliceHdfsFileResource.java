package com.splicemachine.derby.impl.store.access.base;

import java.io.IOException;
import java.io.OutputStream;
import java.io.SyncFailedException;

import org.apache.hadoop.fs.FSDataOutputStream;

import com.splicemachine.db.io.StorageFactory;

/**
 * A FileResource implementation for writing and reading JAR files to and from the Hadoop distributed file system (HDFS).
 *
 * @author dwinters
 */

public class SpliceHdfsFileResource extends SpliceBaseFileResource {

	public SpliceHdfsFileResource(StorageFactory storageFactory) {
		super(storageFactory);
	}

	@Override
	protected void syncOutputStream(OutputStream os) throws IOException {
		((FSDataOutputStream) os).hsync();
	}
}
