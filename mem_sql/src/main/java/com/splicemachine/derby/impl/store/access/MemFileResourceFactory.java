package com.splicemachine.derby.impl.store.access;

import com.splicemachine.db.iapi.store.access.FileResource;
import com.splicemachine.db.io.StorageFactory;
import com.splicemachine.derby.impl.store.access.base.SpliceLocalFileResource;

/**
 * @author Scott Fines
 *         Date: 1/11/16
 */
public class MemFileResourceFactory implements FileResourceFactory{
    @Override
    public FileResource newFileResource(StorageFactory storageFactory){
        return new SpliceLocalFileResource(storageFactory);
    }
}
