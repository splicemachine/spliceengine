package com.splicemachine.derby.impl.load;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.VTIOperation;
import com.splicemachine.derby.vti.SpliceFileVTI;
import com.splicemachine.pipeline.exception.ErrorState;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.utils.file.DefaultFileInfo;
import com.splicemachine.utils.file.FileInfo;
import com.splicemachine.db.iapi.error.StandardException;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 2/26/14
 */
public class ImportUtils {

    /**
     * Return the total space consumed by the import data files.
     *
     * @return total space consumed by the import data files
     *
     * @throws IOException
     */
    public static ContentSummary getImportDataSize(Path filePath) throws IOException {
        FileSystem fs = null;
        try {
            fs = FileSystem.get(SpliceConstants.config);
            ContentSummary summary = fs.getContentSummary(filePath);
            return summary;
        } finally {
            if (fs!=null)
                fs.close();
        }
    }



    public static Path generateFileSystemPathForWrite(String badDirectory, FileSystem fileSystem, SpliceOperation spliceOperation) throws StandardException {
        try {
            String vtiFileName = spliceOperation.getVTIFileName();
            Path badDirectoryPath = new Path(badDirectory);
            validateWritable(badDirectoryPath, fileSystem, true);
            int i = 0;
            while (true) {
                String fileName = new Path(vtiFileName).getName();
                fileName = fileName + (i==0?".bad":"("+i+").bad");
                Path fileSystemPathForWrites = new Path(badDirectoryPath, fileName);
                if (!fileSystem.exists(fileSystemPathForWrites))
                    return fileSystemPathForWrites;
                i++;
            }
        } catch (IOException ioe) {
            throw StandardException.plainWrapException(ioe);
        }
    }

    /**
     *
     * Check that the badLogDirectory exists and is writable
     *
     * @param path
     * @param fileSystem
     * @param checkDirectory
     * @throws StandardException
     */
		public static void validateReadable(Path path,FileSystem fileSystem,boolean checkDirectory) throws StandardException {
				//check that the badLogDirectory exists and is writable
				if(path!=null){
						FileInfo badLogInfo = new DefaultFileInfo(fileSystem);
						try{
								if(checkDirectory && !badLogInfo.isDirectory(path))
										throw ErrorState.LANG_FILE_DOES_NOT_EXIST.newException(path.toString());
								if(!badLogInfo.isReadable(path)){
										String[] ugi = badLogInfo.getUserAndGroup();
										throw ErrorState.LANG_NO_READ_PERMISSION.newException(ugi[0],ugi[1],path.toString());
								}
						}catch(FileNotFoundException fnfe){
								throw Exceptions.parseException(fnfe);
						}catch(IOException ioe){
								throw Exceptions.parseException(ioe);
						}
				}
		}

    /**
     *
     * ValidateWritable FileSystem
     *
     * @param path
     * @param fileSystem
     * @param checkDirectory
     * @throws StandardException
     */
		public static void validateWritable(Path path,FileSystem fileSystem,boolean checkDirectory) throws StandardException {
				//check that the badLogDirectory exists and is writable
				if(path!=null){
						FileInfo badLogInfo = new DefaultFileInfo(fileSystem);
						try{
								if(checkDirectory && !badLogInfo.isDirectory(path))
										throw ErrorState.LANG_NOT_A_DIRECTORY.newException(path.toString());
								if(!badLogInfo.isWritable(path)){
										String[] ugi = badLogInfo.getUserAndGroup();
										throw ErrorState.LANG_NO_WRITE_PERMISSION.newException(ugi[0],ugi[1],path.toString());
								}
						}catch(FileNotFoundException fnfe){
								throw Exceptions.parseException(fnfe);
						}catch(IOException ioe){
								throw Exceptions.parseException(ioe);
						}
				}
		}

}
