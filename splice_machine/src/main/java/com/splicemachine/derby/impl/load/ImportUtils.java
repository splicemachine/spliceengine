package com.splicemachine.derby.impl.load;

import com.splicemachine.access.api.DistributedFileSystem;
import com.splicemachine.access.api.FileInfo;
import com.splicemachine.access.api.DistributedFileOpenOption;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.pipeline.ErrorState;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.impl.driver.SIDriver;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Path;

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
    public static FileInfo getImportFileInfo(String filePath) throws IOException{
        DistributedFileSystem fsLayer=SIDriver.driver().fileSystem();
        return fsLayer.getInfo(filePath);
    }

    public static void validateReadable(String file,boolean checkDirectory) throws StandardException{
        //check that the badLogDirectory exists and is writable
        if(file==null) return;

        DistributedFileSystem fsLayer=SIDriver.driver().fileSystem();
        FileInfo info;
        try{
            info=fsLayer.getInfo(file);
        }catch(IOException e){
            throw Exceptions.parseException(e);
        }
        if(checkDirectory && info.isDirectory())
            throw ErrorState.LANG_FILE_DOES_NOT_EXIST.newException(file);
        if(!info.isReadable()){
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
        DistributedFileSystem fsLayer = SIDriver.driver().fileSystem();
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
}
