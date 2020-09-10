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
/**********************************************************************
 *
 *
 *  Component Name =
 *
 *  Package Name = com.splicemachine.db.client.net
 *
 *  Descriptive Name = XACallInfo class
 *
 *  Function = Handle XA information
 *
 *  List of Classes
 *              - NetXACallInfo
 *
 *  Restrictions : None
 *
 **********************************************************************/
package com.splicemachine.db.client.net;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

public class NetXACallInfo {
    Xid xid_;                         // current xid
    int xaFlags_;                     // current xaFlags
    /** XA transaction timeout in milliseconds. The value less than 0 means
      * that the time out is not specified. The value 0 means infinite timeout. */
    long xaTimeoutMillis_;
    // may not be needed!!!~~~
    int xaFunction_;                  // queued XA function being performed
    int xaRetVal_;                    // xaretval from server
    NetXAConnection actualConn_; // the actual connection object, not necessarily
    // the user's connection object
    /* only the first connection object is actually used. The other connection
     * objects are used only for their TCP/IP variables to simulate
     * suspend / resume
     */

    private byte[] crrtkn_;
    private java.io.InputStream in_;
    private java.io.OutputStream out_;

    private byte[] uowid_;  // Unit of Work ID

    private boolean readOnlyTransaction_;  // readOnlyTransaction Flag

    public NetXACallInfo() {
        xid_ = null;
        xaFlags_ = XAResource.TMNOFLAGS;
        xaTimeoutMillis_ = -1;
        actualConn_ = null;
        readOnlyTransaction_ = true;
        xaRetVal_ = 0;
    }

    public NetXACallInfo(Xid xid, int flags, NetXAConnection actualConn) {
        xid_ = xid;
        xaFlags_ = flags;
        xaTimeoutMillis_ = -1;
        actualConn_ = actualConn;
        readOnlyTransaction_ = true;
        xaRetVal_ = 0;
    }

    public void saveConnectionVariables() {
        in_ = actualConn_.getNetConnection().getInputStream();
        out_ = actualConn_.getNetConnection().getOutputStream();
        crrtkn_ = actualConn_.getCorrelatorToken();
    }

    public java.io.InputStream getInputStream() {
        return in_;
    }

    public java.io.OutputStream getOutputStream() {
        return out_;
    }

    public byte[] getCorrelatorToken() {
        return crrtkn_;
    }

    protected void setUOWID(byte[] uowid) {
        uowid_ = uowid;
    }

    protected byte[] getUOWID() {
        return uowid_;
    }

    protected void setReadOnlyTransactionFlag(boolean flag) {
        readOnlyTransaction_ = flag;
    }

    protected boolean getReadOnlyTransactionFlag() {
        return readOnlyTransaction_;
    }


}









