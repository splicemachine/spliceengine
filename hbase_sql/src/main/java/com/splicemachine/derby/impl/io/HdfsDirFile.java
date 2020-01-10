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

package com.splicemachine.derby.impl.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.io.StorageFile;
import com.splicemachine.db.io.StorageRandomAccessFile;

/**
 * This class provides a HDFS based implementation of the StorageFile interface. It is used by the
 * database engine to access persistent JAR files under the jars directory in HDFS.
 *
 * @author dwinters
 */
class HdfsDirFile implements StorageFile {
	private static final Logger LOG = Logger.getLogger(HdfsDirFile.class);
	private String path;
	private FileSystem fileSystem;

	/**
	 * Construct a HdfsDirFile from a path name.
	 *
	 * @param path The path name.
	 */
	HdfsDirFile(String path) {
		if (path == null) {
			throw new IllegalArgumentException("The argument 'path' cannot be null.");
		}
		this.path = path;
	}

	/**
	 * Construct a HdfsDirFile from a directory name and a file name.
	 *
	 * @param directoryName The directory part of the path name.
	 * @param fileName The name of the file within the directory.
	 */
	HdfsDirFile(String directoryName, String fileName) {
		if (fileName == null) {
			throw new IllegalArgumentException("The argument 'path' cannot be null.");
		}
		if (directoryName == null || directoryName.isEmpty()) {
			path = FileSystem.getDefaultUri(HConfiguration.unwrapDelegate()).getPath() +
                File.separatorChar + fileName;
		} else {
			path = directoryName + File.separatorChar + fileName;
		}
	}

	/**
	 * Construct a HdfsDirFile from a directory name and a file name.
	 *
	 * @param directoryName The directory part of the path name.
	 * @param fileName The name of the file within the directory.
	 */
	HdfsDirFile(HdfsDirFile directoryName, String fileName) {
		this(directoryName == null ? null : directoryName.getPath(), fileName);
	}

	/**
	 * Set the file system.
	 *
	 * @return the file system
	 * @throws IOException
	 */
	public FileSystem getFileSystem() throws IOException {
		if (fileSystem == null) {
			fileSystem = FileSystem.get(HConfiguration.unwrapDelegate());
		}
		return fileSystem;
	}

	/**
	 * Get the file system.
	 * If it has not been set, then get the file system for the Splice configuration.
	 *
	 * @param fileSystem the file system to set
	 */
	public void setFileSystem(FileSystem fileSystem) {
		this.fileSystem = fileSystem;
	}

	@Override
	public String[] list() {
		try {
			FileSystem fs = getFileSystem();
			FileStatus[] fileStatuses = fs.listStatus(new Path(path));
			String[] list = new String[fileStatuses.length];
			for (int i = 0; i < fileStatuses.length; i++) {
				list[i] = fileStatuses[i].getPath().getName();
			}
			return list;
		} catch (IOException e) {
			LOG.error(String.format("An exception occurred while listing the files and directories in the path '%s'.", path), e);
			return null;
		}
	}

	@Override
	public boolean canWrite() {
		// TODO: Not implemented yet.
		// Take a look at FileStatus.getPermissions().
		return true;
	}

	@Override
	public boolean exists() {
		try {
			FileSystem fs = getFileSystem();
			return fs.exists(new Path(path));
		} catch (IOException e) {
			LOG.error(String.format("An exception occurred while checking the existence of the path '%s'.", path), e);
			return false;
		}
	}

	@Override
	public boolean isDirectory() {
		try {
			FileSystem fs = getFileSystem();
			return fs.isDirectory(new Path(path));
		} catch (IOException e) {
			LOG.error(String.format("An exception occurred while checking if the path '%s' is a directory.", path), e);
			return false;
		}
	}

	@Override
	public boolean delete() {
		try {
			FileSystem fs = getFileSystem();
			return fs.delete(new Path(path), false);
		} catch (IOException e) {
			LOG.error(String.format("An exception occurred while deleting the path '%s'.", path), e);
			return false;
		}
	}

	@Override
	public boolean deleteAll() {
		try {
			FileSystem fs = getFileSystem();
			return fs.delete(new Path(path), true);
		} catch (IOException e) {
			LOG.error(String.format("An exception occurred while deleting the path '%s'.", path), e);
			return false;
		}
	}

	@Override
	public String getPath() {
		return path;
	}

	@Override
	public String getCanonicalPath() throws IOException {
		return path;
	}

	@Override
	public String getName() {
		if (path == null || path.isEmpty()) return "";  // As specified in the Javadoc.
		return new Path(path).getName();
	}

	@Override
	public URL getURL() throws MalformedURLException {
		return new Path(path).toUri().toURL();
	}

	@Override
	public boolean createNewFile() throws IOException {
		FSDataOutputStream os = null;
		try {
			FileSystem fs = getFileSystem();
			os = fs.create(new Path(path), false);
			return true;
		} catch (IOException e) {
			LOG.error(String.format("An exception occurred while creating the path '%s'.", path), e);
			return false;
		} finally {
			if (os != null) { os.close(); }
		}
	}

	@Override
	public boolean renameTo(StorageFile newName) {
		try {
			FileSystem fs = getFileSystem();
			boolean renameResult = fs.rename(new Path(path), new Path(newName.getPath()));
			if (renameResult) {
				this.path = newName.getPath();
			}
			return renameResult;
		} catch (IOException e) {
			LOG.error(String.format("An exception occurred while making directories in the path '%s'.", path), e);
			return false;
		}
	}

	@Override
	public boolean mkdir() {
		return mkdirs();  // HDFS doesn't have a 'mkdir', but only 'mkdirs'.
	}

	@Override
	public boolean mkdirs() {
		try {
			FileSystem fs = getFileSystem();
			return fs.mkdirs(new Path(path));
		} catch (IOException e) {
			LOG.error(String.format("An exception occurred while making directories in the path '%s'.", path), e);
			return false;
		}
	}

	@Override
	public long length() {
		if (!exists() || isDirectory()) { return 0; }  // As specified in the Javadoc.
		try {
			FileSystem fs = getFileSystem();
			return fs.getContentSummary(new Path(path)).getLength();
		} catch (IOException e) {
			LOG.error(String.format("An exception occurred while getting the size of the file '%s'.", path), e);
			return 0;
		}
	}

	@Override
	public StorageFile getParentDir() {
		return new HdfsDirFile(new Path(path).getParent().toString());
	}

	@Override
	public boolean setReadOnly() {
		// TODO: Not implemented yet.  This doesn't appear to be used anywhere.
		// Take a look at FileStatus.getPermissions().
		return false;
	}

	@Override
	public OutputStream getOutputStream() throws FileNotFoundException {
		try {
			FileSystem fs = getFileSystem();
			return fs.create(new Path(path), false);
		} catch (FileNotFoundException fnfe) {
			throw fnfe;
		} catch (IOException e) {
			LOG.error(String.format("An exception occurred while creating the file '%s'.", path), e);
			return null;
		}
	}

	@Override
	public OutputStream getOutputStream(boolean append)
			throws FileNotFoundException {
		if (append) {
			try {
				FileSystem fs = getFileSystem();
				return fs.append(new Path(path));
			} catch (FileNotFoundException fnfe) {
				throw fnfe;
			} catch (IOException e) {
				LOG.error(String.format("An exception occurred while creating the file '%s'.", path), e);
				return null;
			}
		} else {
			this.delete();
			return getOutputStream();
		}
	}

	@Override
	public InputStream getInputStream() throws FileNotFoundException {
		try {
			FileSystem fs = getFileSystem();
			return fs.open(new Path(path));
		} catch (FileNotFoundException fnfe) {
			throw fnfe;
		} catch (IOException e) {
			LOG.error(String.format("An exception occurred while opening the file '%s'.", path), e);
			return null;
		}
	}

	@Override
	public int getExclusiveFileLock() throws StandardException {
		return NO_FILE_LOCK_SUPPORT;
	}

	@Override
	public void releaseExclusiveFileLock() {
	}

	@Override
	public StorageRandomAccessFile getRandomAccessFile(String mode)
			throws FileNotFoundException {
		return null;  // Not supported in HDFS.
	}

	@Override
	public void limitAccessToOwner() {
		// TODO: Not implemented yet.
		// Take a look at FileStatus.getPermissions().
	}
}
