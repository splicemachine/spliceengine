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

package com.splicemachine.derby.impl.io;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import com.splicemachine.access.HConfiguration;

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
		if (home == null) {
			/*
			 * For a DFS install, the jars will be written under the /hbase directory since it is the only directory
			 * that is always writable by the hbase user in the DFS.
			 * Otherwise, it's a standalone install and the jars will be stored in directories off of the splice install directory.
			 */
			String defaultFS = HConfiguration.unwrapDelegate().get("fs.defaultFS");
			if (defaultFS != null && (defaultFS.startsWith("hdfs://") || defaultFS.startsWith("maprfs://"))) {
				home = "/hbase";
			}
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
			canonicalName = databaseRoot.getPath();
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
	public void sync(OutputStream stream, boolean metaData) throws IOException{
		stream.flush();
	}

	@Override
	public boolean supportsWriteSync() {
		return false;
	}
}
