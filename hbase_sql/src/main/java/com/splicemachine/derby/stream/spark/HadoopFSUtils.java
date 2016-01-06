package com.splicemachine.derby.stream.spark;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.pipeline.ErrorState;
import com.splicemachine.pipeline.Exceptions;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.HFile;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 1/11/16
 */
public class HadoopFSUtils{

    public static Path generateFileSystemPathForWrite(String badDirectory,
                                                      FileSystem fileSystem,
                                                      SpliceOperation spliceOperation) throws StandardException{
        try{
            String vtiFileName=spliceOperation.getVTIFileName();
            Path badDirectoryPath=new Path(badDirectory);
            validateWritable(badDirectoryPath,fileSystem,true);
            int i=0;
            while(true){
                String fileName=new Path(vtiFileName).getName();
                fileName=fileName+(i==0?".bad":"("+i+").bad");
                Path fileSystemPathForWrites=new Path(badDirectoryPath,fileName);
                if(!fileSystem.exists(fileSystemPathForWrites))
                    return fileSystemPathForWrites;
                i++;
            }
        }catch(IOException ioe){
            throw StandardException.plainWrapException(ioe);
        }
    }

    /**
     * Check that the badLogDirectory exists and is writable
     *
     * @param path
     * @param fileSystem
     * @param checkDirectory
     * @throws StandardException
     */
    public static void validateReadable(Path path,FileSystem fileSystem,boolean checkDirectory) throws StandardException{
        //check that the badLogDirectory exists and is writable
        if(path!=null){
            FileInfo badLogInfo=new DefaultFileInfo(fileSystem);
            try{
                if(checkDirectory && !badLogInfo.isDirectory(path))
                    throw ErrorState.LANG_FILE_DOES_NOT_EXIST.newException(path.toString());
                if(!badLogInfo.isReadable(path)){
                    String[] ugi=badLogInfo.getUserAndGroup();
                    throw ErrorState.LANG_NO_READ_PERMISSION.newException(ugi[0],ugi[1],path.toString());
                }
            }catch(IOException ioe){
                throw Exceptions.parseException(ioe);
            }
        }
    }

    /**
     * ValidateWritable FileSystem
     *
     * @param path
     * @param fileSystem
     * @param checkDirectory
     * @throws StandardException
     */
    public static void validateWritable(Path path,FileSystem fileSystem,boolean checkDirectory) throws StandardException{
        //check that the badLogDirectory exists and is writable
        if(path!=null){
            FileInfo badLogInfo=new DefaultFileInfo(fileSystem);
            try{
                if(checkDirectory && !badLogInfo.isDirectory(path))
                    throw ErrorState.LANG_NOT_A_DIRECTORY.newException(path.toString());
                if(!badLogInfo.isWritable(path)){
                    String[] ugi=badLogInfo.getUserAndGroup();
                    throw ErrorState.LANG_NO_WRITE_PERMISSION.newException(ugi[0],ugi[1],path.toString());
                }
            }catch(IOException ioe){
                throw Exceptions.parseException(ioe);
            }
        }
    }
}
