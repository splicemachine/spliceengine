package com.splicemachine.derby.impl.store.access;

import com.splicemachine.db.iapi.store.access.FileResource;

/**
 * @author Scott Fines
 *         Date: 1/7/16
 */
public interface FileResourceFactory{

    FileResource newFileResource();
}
