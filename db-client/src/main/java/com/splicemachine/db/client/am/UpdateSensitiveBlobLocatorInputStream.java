/*
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.client.am;

import java.io.InputStream;

/**
 * This class extends from the UpdateSensitiveLOBLocatorInputStream
 * and creates and returns an implementation of the Blob specific
 * locator InputStream. It also over-rides the reCreateStream method
 * which re-creates the underlying Blob locator stream whenever a
 * update happens on the Blob object associated with this stream.
 */
public class UpdateSensitiveBlobLocatorInputStream 
        extends UpdateSensitiveLOBLocatorInputStream {
    //Stores the Blob instance associated with
    //this InputStream.
    private Blob blob = null;
    
    /**
     * Creates an instance of the BlobLocatorInputStream
     * and and calls the super class constructors with 
     * appropriate initializers.
     *
     * @param con connection to be used to read the
     *        <code>Blob</code> value from the server
     * @param blob <code>Blob</code> object that contains locator for
     *        the <code>Blob</code> value on the server.
     *
     * @throws SqlException If any exception occurs during stream
     *                      creation.
     */
    public UpdateSensitiveBlobLocatorInputStream(ClientConnection con, Blob blob)
    throws SqlException {
        super(con, blob, new BlobLocatorInputStream(con, blob));
        this.blob = blob;
    }
    
    /**
     * Creates an instance of the BlobLocatorInputStream.
     *
     * @param con connection to be used to read the
     *        <code>Blob</code> value from the server
     * @param blob <code>Blob</code> object that contains locator for
     *        the <code>Blob</code> value on the server.
     * @param position the position in the <code>Blob</code> of the first
     *        byte to read.
     * @param length the maximum number of bytes to read from
     *        the <code>Blob</code>.
     *
     * @throws SqlException If any exception occurs during stream
     *                      creation.
     */
    public UpdateSensitiveBlobLocatorInputStream(ClientConnection con, Blob blob,
                                                 long position, long length) throws SqlException {
        super(con, blob, 
                new BlobLocatorInputStream(con, blob, position, length), 
                position, length);
        this.blob = blob;
    }
    
    /**
     * Re-creates the underlying Locator stream
     * with the current position and the length
     * values if specified.
     *
     * @throws SqlException If any exception occurs while
     *                      re-creating the underlying streams.
     */
    protected InputStream reCreateStream() throws SqlException {
        InputStream is_ = null;
        
        //Verify if a subset of the underlying
        //LOB data was requested.
        if(length != -1) {
            //The length information is available.
            //It should be used while re-creating
            //the stream.
            is_ = new BlobLocatorInputStream(con, blob, currentPos, 
                    length - currentPos +1);
        }
        else {
            //The length information is not
            //available.
            is_ = new BlobLocatorInputStream(con, blob, currentPos, -1);
        }
        return is_;
    }
}
