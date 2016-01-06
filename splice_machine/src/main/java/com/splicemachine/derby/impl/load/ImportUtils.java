package com.splicemachine.derby.impl.load;

import com.splicemachine.access.api.DistributedFileSystem;
import com.splicemachine.access.api.FileInfo;
import com.splicemachine.access.api.DistributedFileOpenOption;
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
    public static FileInfo getImportDataSize(String filePath) throws IOException{
//        FileSystem fs = FileSystem.get(SpliceConstants.config);
        DistributedFileSystem fsLayer=SIDriver.driver().fileSystem();
        return fsLayer.getInfo(filePath);
//        ContentSummary summary=fs.getContentSummary(filePath);
//        return summary;
    }




}
