package com.splicemachine.derby.impl.store.access;

import com.splicemachine.db.iapi.store.access.FileResource;
import com.splicemachine.db.io.StorageFactory;
import com.splicemachine.derby.impl.store.access.base.SpliceHdfsFileResource;

/**
 * @author Scott Fines
 *         Date: 1/27/16
 */
public class HBaseFileResourceFactory implements FileResourceFactory{
    @Override
    public FileResource newFileResource(StorageFactory storageFactory){
        return new SpliceHdfsFileResource(storageFactory);
    }
}
