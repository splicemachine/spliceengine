package com.splicemachine.derby.impl.load;

import org.apache.hadoop.fs.FileSystem;

import java.io.Closeable;
import java.io.Externalizable;
import java.io.IOException;

/**
 * @author Scott Fines
 * Created on: 9/30/13
 */
public interface ImportReader extends Closeable,Externalizable {

    void setup(FileSystem fileSystem,ImportContext ctx) throws IOException;

    public String[] nextRow() throws IOException;

}
