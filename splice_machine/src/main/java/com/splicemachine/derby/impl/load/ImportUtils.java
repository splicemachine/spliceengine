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


    private static DistributedFileSystem getFileSystem(String path) throws StandardException {
        try {
           return SIDriver.driver().getSIEnvironment().fileSystem(path);
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
    }
}
