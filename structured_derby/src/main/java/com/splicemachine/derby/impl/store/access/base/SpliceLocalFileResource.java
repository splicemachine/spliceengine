package com.splicemachine.derby.impl.store.access.base;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.store.access.FileResource;
import org.apache.derby.impl.io.DirStorageFactory4;
import org.apache.derby.io.StorageFactory;
import org.apache.derby.io.StorageFile;

/**
 * A very simple FileResource implementation for writing and reading JAR files from the local file system.
 *
 * @author dwinters
 */

//
// =========================
// Internal comments...
//=========================
//
// This is a super gutted version of the Derby RFResource class.  Most of the logic for writing and reading JAR files
// is self-contained in this class.  The Derby implementation included close to a dozen or so factories that are each
// about 3K lines long.  Unfortunately, most of that logic is related to dealing with transactions, logging, and
// reading/writing from/to raw files.  We have replaced those factories with just a couple of Splice factories that
// move the burden of transactions, logging, and I/O to HBase (co-processors, observers, etc.).
//
// The current implementation of JAR file storage for Splice does not include versioning, transactions, logging, or
// backup/restore.  It also does not support running in a clustered setup of Splice since the JAR files are stored to
// the local file system.  A future version of this simple JAR file storage system could simply store the JAR files to
// HDFS to allow all Splice nodes to access the JAR files.  And the final product should include full transactional
// support and support for backup and recovery of the JAR files.
//

public class SpliceLocalFileResource implements FileResource {

	private StorageFactory storageFactory;

	public SpliceLocalFileResource(StorageFactory storageFactory) {
		this.storageFactory = storageFactory;
	}

	/* (non-Javadoc)
	 * @see org.apache.derby.iapi.store.access.FileResource#add(java.lang.String, java.io.InputStream)
	 */
	@Override
	public long add(String name, InputStream source) throws StandardException {
		OutputStream os = null;
		long generationId = 0;  // Hard coded to 0 to avoid needing a DataFactory.

		try
		{
			StorageFile file = getAsFile(name, generationId);
            if (file.exists())
            {
				throw StandardException.newException(
                        SQLState.FILE_EXISTS, file);
            }

			StorageFile directory = file.getParentDir();
            StorageFile parentDir = directory.getParentDir();
            boolean pdExisted = parentDir.exists();

            if (!directory.exists())
			{
                if (!directory.mkdirs())
                {
					throw StandardException.newException(
                            SQLState.FILE_CANNOT_CREATE_SEGMENT, directory);
                }

                directory.limitAccessToOwner();

                if (!pdExisted) {
                    parentDir.limitAccessToOwner();
                }
			}

            os = file.getOutputStream();
			byte[] data = new byte[4096];
			int len;

			while ((len = source.read(data)) != -1) {
				os.write(data, 0, len);
			}
			((FileOutputStream) os).getFD().sync();
		}

		catch (IOException ioe)
		{
			throw StandardException.newException(
                    SQLState.FILE_UNEXPECTED_EXCEPTION, ioe);
		}

		finally
		{
			try {
				if (os != null) {
					os.close();
				}
			} catch (IOException ioe2) {/*RESOLVE: Why ignore this?*/}

			try {
				if (source != null)source.close();
			} catch (IOException ioe2) {/* RESOLVE: Why ignore this?*/}
		}

		return generationId;
	}

	/* (non-Javadoc)
	 * @see org.apache.derby.iapi.store.access.FileResource#remove(java.lang.String, long)
	 */
	@Override
	public void remove(String name, long currentGenerationId)
			throws StandardException {
		StorageFile fileToGo = getAsFile(name, currentGenerationId);

		if (fileToGo.exists()) {
            if (fileToGo.isDirectory()) {
                if (!fileToGo.deleteAll()) {
                    throw StandardException.newException(
                            SQLState.FILE_CANNOT_REMOVE_FILE, fileToGo);
                }
            } else {
                if (!fileToGo.delete()) {
                    throw StandardException.newException(
                            SQLState.FILE_CANNOT_REMOVE_FILE, fileToGo);
                }
            }
        }
	}

	/* (non-Javadoc)
	 * @see org.apache.derby.iapi.store.access.FileResource#removeJarDir(java.lang.String)
	 */
	@Override
	public void removeJarDir(String f) throws StandardException {
		//
		// -----------------------------------------------
		// PLEASE NOTE: Purposefully not implemented.
		// -----------------------------------------------
		// The only place where this method is invoked is by some upgrade code.
		// It seems like a bad idea to blow away a customer's JAR files even during an upgrade.
		// We can re-evaluate the need for this method when we tackle the upgrade procedure.
		//
	}

	/* (non-Javadoc)
	 * @see org.apache.derby.iapi.store.access.FileResource#replace(java.lang.String, long, java.io.InputStream)
	 */
	@Override
	public long replace(String name, long currentGenerationId,
			InputStream source) throws StandardException {
		remove(name, currentGenerationId);
		long generationId = add(name, source);
		return generationId;
	}

	/* (non-Javadoc)
	 * @see org.apache.derby.iapi.store.access.FileResource#getAsFile(java.lang.String, long)
	 */
	@Override
	public StorageFile getAsFile(String name, long generationId) {
		String versionedFileName = getVersionedName(name, generationId);
		return storageFactory.newStorageFile(versionedFileName);
	}

	/* (non-Javadoc)
	 * @see org.apache.derby.iapi.store.access.FileResource#getSeparatorChar()
	 */
	@Override
	public char getSeparatorChar() {
        // JAR files are always java.io.File's and use its separator.
        return File.separatorChar;
	}

	// Moved from BaseDataFileFactory to avoid needing a DataFactory, which would then require a half dozen or more factories
	// that aren't really needed for simple writing and reading of local JAR files.
	private String getVersionedName(String name, long generationId) {
		return name.concat(".G".concat(Long.toString(generationId)));
	}
}
