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

package com.splicemachine.derby.impl.store.access.base;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.store.access.FileResource;
import com.splicemachine.db.io.StorageFactory;
import com.splicemachine.db.io.StorageFile;

import java.io.*;

/**
 * A very simple FileResource implementation for writing and reading JAR files to and from a file system.
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

public abstract class SpliceBaseFileResource implements FileResource{

    private StorageFactory storageFactory;

    public SpliceBaseFileResource(StorageFactory storageFactory){
        this.storageFactory=storageFactory;
    }

    @Override
    public long add(String name,InputStream source) throws StandardException{
        OutputStream os=null;
        long generationId=0;  // Hard coded to 0 to avoid needing a DataFactory.

        try{
            StorageFile file=getAsFile(name,generationId);
            if(file.exists()){
                throw StandardException.newException(
                        SQLState.FILE_EXISTS,file);
            }

            StorageFile directory=file.getParentDir();
            StorageFile parentDir=directory.getParentDir();
            boolean pdExisted=parentDir.exists();

            if(!directory.exists()){
                if(!directory.mkdirs()){
                    throw StandardException.newException(
                            SQLState.FILE_CANNOT_CREATE_SEGMENT,directory);
                }

                directory.limitAccessToOwner();

                if(!pdExisted){
                    parentDir.limitAccessToOwner();
                }
            }

            os=file.getOutputStream();
            byte[] data=new byte[4096];
            int len;

            while((len=source.read(data))!=-1){
                os.write(data,0,len);
            }
            syncOutputStream(os);
        }catch(IOException ioe){
            throw StandardException.newException(
                    SQLState.FILE_UNEXPECTED_EXCEPTION,ioe);
        }finally{
            try{
                if(os!=null){
                    os.close();
                }
            }catch(IOException ioe2){/*RESOLVE: Why ignore this?*/}

            try{
                if(source!=null) source.close();
            }catch(IOException ioe2){/* RESOLVE: Why ignore this?*/}
        }

        return generationId;
    }

    /**
     * Syncs the output stream with its durable data store.
     *
     * @param os output stream
     * @throws SyncFailedException
     * @throws IOException
     */
    protected abstract void syncOutputStream(OutputStream os) throws IOException;

    @Override
    public void remove(String name,long currentGenerationId) throws StandardException{
        StorageFile fileToGo=getAsFile(name,currentGenerationId);

        if(fileToGo.exists()){
            if(fileToGo.isDirectory()){
                if(!fileToGo.deleteAll()){
                    throw StandardException.newException(
                            SQLState.FILE_CANNOT_REMOVE_FILE,fileToGo);
                }
            }else{
                if(!fileToGo.delete()){
                    throw StandardException.newException(
                            SQLState.FILE_CANNOT_REMOVE_FILE,fileToGo);
                }
            }
        }
    }

    @Override
    public void removeJarDir(String f) throws StandardException{
        //
        // -----------------------------------------------
        // PLEASE NOTE: Purposefully not implemented.
        // -----------------------------------------------
        // The only place where this method is invoked is by some upgrade code.
        // It seems like a bad idea to blow away a customer's JAR files even during an upgrade.
        // We can re-evaluate the need for this method when we tackle the upgrade procedure.
        //
    }

    @Override
    public long replace(String name,long currentGenerationId,InputStream source) throws StandardException{
        remove(name,currentGenerationId);
        return add(name,source);
    }

    @Override
    public StorageFile getAsFile(String name,long generationId){
        String versionedFileName=getVersionedName(name,generationId);
        return storageFactory.newStorageFile(versionedFileName);
    }

    @Override
    public char getSeparatorChar(){
        // JAR files are always java.io.File's and use its separator.
        return File.separatorChar;
    }

    // Moved from BaseDataFileFactory to avoid needing a DataFactory, which would then require a half dozen or more factories
    // that aren't really needed for simple writing and reading of local JAR files.
    private String getVersionedName(String name,long generationId){
        return name + ".G".concat(Long.toString(generationId));
    }

    public StorageFactory getStorageFactory() {
        return storageFactory;
    }
}
