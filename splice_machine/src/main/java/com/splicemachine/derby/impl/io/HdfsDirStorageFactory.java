package com.splicemachine.derby.impl.io;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.SyncFailedException;

import com.splicemachine.db.io.StorageFactory;
import com.splicemachine.db.io.StorageFile;
import com.splicemachine.db.io.WritableStorageFactory;

/**
 * This class provides a HDFS based implementation of the StorageFactory interface. It is used by the
 * database engine to access persistent JAR files under the jars directory in HDFS.
 */

public class HdfsDirStorageFactory implements WritableStorageFactory
{
	String home;
	protected String dataDirectory;
	protected String separatedDataDirectory; // dataDirectory + separator
	protected String uniqueName;
	protected String canonicalName;

	/**
	 * Most of the initialization is done in the init method.
	 */
	public HdfsDirStorageFactory()
	{
		super();
	}

	@Override
	public void init(String home, String databaseName, String tempDirName,
			String uniqueName) throws IOException {
		if (databaseName != null)
		{
			dataDirectory = databaseName;
			separatedDataDirectory = databaseName + getSeparator();
		}
		this.home = home;
		this.uniqueName = uniqueName;
		doInit();
	}

	void doInit() throws IOException
	{
		if (dataDirectory != null)
		{
			File dataDirectoryFile = new File(dataDirectory);
			File databaseRoot = null;
			if (dataDirectoryFile.isAbsolute())
			{
				databaseRoot = dataDirectoryFile;
			}
			else if (home != null && dataDirectory.startsWith(home))
			{
				databaseRoot = dataDirectoryFile;
			}
			else
			{
				databaseRoot = new File(home, dataDirectory);
				if (home != null) {
					dataDirectory = home + getSeparator() +  dataDirectory;
				}
			}
			canonicalName = databaseRoot.getCanonicalPath();
			separatedDataDirectory = dataDirectory + getSeparator();
		}
		else if (home != null)
		{
			File root = new File(home);
			dataDirectory = root.getCanonicalPath();
			separatedDataDirectory = dataDirectory + getSeparator();
		}
	} // end of doInit

	@Override
	public void shutdown() {
	}

	@Override
	public String getCanonicalName() throws IOException {
		return canonicalName;
	}

	@Override
	public StorageFile newStorageFile(String path) {
		if (path == null)
			return new HdfsDirFile(dataDirectory);
		return new HdfsDirFile(dataDirectory, path);
	}

	@Override
	public StorageFile newStorageFile(String directoryName, String fileName) {
		return new HdfsDirFile(separatedDataDirectory + directoryName, fileName);
	}

	@Override
	public StorageFile newStorageFile(StorageFile directoryName, String fileName) {
		return new HdfsDirFile((HdfsDirFile) directoryName, fileName);
	}

	@Override
	public char getSeparator() {
		return File.separatorChar;
	}

	@Override
	public StorageFile getTempDir() {
		return null;
	}

	@Override
	public boolean isFast() {
		return false;
	}

	@Override
	public boolean isReadOnlyDatabase() {
		return false;
	}

	@Override
	public boolean supportsRandomAccess() {
		return false;
	}

	@Override
	public int getStorageFactoryVersion() {
		return StorageFactory.VERSION_NUMBER;
	}

	@Override
	public StorageFile createTemporaryFile(String prefix, String suffix)
			throws IOException {
		return null;
	}

	@Override
	public void setCanonicalName(String name) {
		canonicalName = name;
	}

	@Override
	public void sync(OutputStream stream, boolean metaData) throws IOException,
	SyncFailedException {
		stream.flush();
	}

	@Override
	public boolean supportsWriteSync() {
		return false;
	}
}
