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

package com.splicemachine.derby.impl.load;

import com.splicemachine.access.api.DistributedFileSystem;
import com.splicemachine.access.api.FileInfo;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.pipeline.ErrorState;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.impl.driver.SIDriver;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 2/26/14
 */
public class ImportUtils{

    /**
     * Return the total space consumed by the import data files.
     *
     * @return total space consumed by the import data files
     * @throws IOException
     */
    public static FileInfo getImportFileInfo(String filePath) throws StandardException, IOException{
        DistributedFileSystem fsLayer=getFileSystem(filePath);
        return fsLayer.getInfo(filePath);
    }

    /**
     * Check that the file given by the string path (or directory, if flagged) exists and is readable.
     * @param file the absolute path to the file
     * @param checkDirectory check as a directory if this flag set
     * @throws StandardException
     */
    public static void validateReadable(String file,boolean checkDirectory) throws StandardException{
        if(file==null) return;

        DistributedFileSystem fsLayer=getFileSystem(file);
        FileInfo info;
        try{
            info=fsLayer.getInfo(file);
        }catch(IOException e){
            throw Exceptions.parseException(e);
        }
        if(! info.exists() || (checkDirectory && info.isDirectory()))
            throw ErrorState.LANG_FILE_DOES_NOT_EXIST.newException(file);
        if(! info.isReadable()){
            throw ErrorState.LANG_NO_READ_PERMISSION.newException(info.getUser(),info.getGroup(),file);
        }
    }

    /**
     * ValidateWritable FileSystem
     *
     * @param path the path to check
     * @param checkDirectory if true, then ensure that the file specified by {@code path} is not
     *                       a directory
     * @throws StandardException
     */
    public static void validateWritable(String path,boolean checkDirectory) throws StandardException{
        //check that the badLogDirectory exists and is writable
        if(path==null) return;
        DistributedFileSystem fsLayer = getFileSystem(path);
        FileInfo info;
        try{
            info = fsLayer.getInfo(path);
        }catch(IOException e){
            throw Exceptions.parseException(e);
        }

        if(checkDirectory &&!info.isDirectory()){
            throw ErrorState.DATA_FILE_NOT_FOUND.newException(path);
        }
        if(!info.isWritable()){
            throw ErrorState.LANG_NO_WRITE_PERMISSION.newException(info.getUser(),info.getGroup(),path);
        }
    }


    public static DistributedFileSystem getFileSystem(String path) throws StandardException {
        try {
           return SIDriver.driver().getSIEnvironment().fileSystem(path);
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
    }
}
