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

package com.splicemachine.db.client.net;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import com.splicemachine.db.client.am.ConnectionCallbackInterface;
import com.splicemachine.db.client.am.DisconnectException;

public class NetXAConnectionReply extends NetResultSetReply {
    NetXAConnectionReply(NetAgent netAgent, int bufferSize) {
        super(netAgent, bufferSize);
    }
    //----------------------------- entry points ---------------------------------


    public void readLocalXAStart(ConnectionCallbackInterface connection) throws DisconnectException {
    }

    public void readLocalXACommit(ConnectionCallbackInterface connection) throws DisconnectException {

        startSameIdChainParse();
        parseSYNCCTLreply(connection);
        endOfSameIdChainData();

        connection.completeLocalCommit();
    }

    public void readLocalXARollback(ConnectionCallbackInterface connection) throws DisconnectException {
        startSameIdChainParse();
        parseSYNCCTLreply(connection);
        endOfSameIdChainData();
        connection.completeLocalRollback();
    }

    protected void readXaStartUnitOfWork(NetConnection conn) throws DisconnectException {
        startSameIdChainParse();
        parseSYNCCTLreply(conn);
        // If we are joining or resuming a global transaction, we let the
        // server set the transcation isolation state for us.
        // Otherwise we do a normal reset.
        NetXACallInfo callInfo =
                conn.xares_.callInfoArray_[conn.currXACallInfoOffset_];
        boolean keep = callInfo.xaFlags_ == XAResource.TMJOIN ||
                callInfo.xaFlags_ == XAResource.TMRESUME;
        conn.xares_.setKeepCurrentIsolationLevel(keep);
        endOfSameIdChainData();
    }

    protected int readXaEndUnitOfWork(NetConnection conn) throws DisconnectException {
        // We have ended the XA unit of work, the next logical connection
        // should be reset using the normal procedure.
        conn.xares_.setKeepCurrentIsolationLevel(false);
        NetXACallInfo callInfo = conn.xares_.callInfoArray_[conn.currXACallInfoOffset_];
        int xaFlags = callInfo.xaFlags_;

        startSameIdChainParse();
        parseSYNCCTLreply(conn);
        endOfSameIdChainData();
        if (xaFlags == XAResource.TMFAIL) {
            return javax.transaction.xa.XAException.XA_RBROLLBACK;
        }
        return javax.transaction.xa.XAResource.XA_OK;
    }

    protected int readXaPrepare(NetConnection conn) throws DisconnectException {
        startSameIdChainParse();
        int synctype = parseSYNCCTLreply(conn);
        endOfSameIdChainData();

        NetXACallInfo callInfo = conn.xares_.callInfoArray_[conn.currXACallInfoOffset_];
        if (synctype == XAResource.XA_RDONLY) { // xaretval of read-only, make sure flag agrees
            callInfo.setReadOnlyTransactionFlag(true);
        } else { // xaretval NOT read-only, make sure flag agrees
            callInfo.setReadOnlyTransactionFlag(false);
        }
        return synctype;
    }

    protected void readXaCommit(NetConnection conn) throws DisconnectException {
        startSameIdChainParse();
        parseSYNCCTLreply(conn);
        endOfSameIdChainData();

        conn.completeLocalCommit();
    }

    protected int readXaRollback(NetConnection conn) throws DisconnectException {
        startSameIdChainParse();
        parseSYNCCTLreply(conn);
        endOfSameIdChainData();

        conn.completeLocalRollback();

        return javax.transaction.xa.XAResource.XA_OK;
    }

    protected void readXaRecover(NetConnection conn) throws DisconnectException {
        startSameIdChainParse();
        parseSYNCCTLreply(conn);
        endOfSameIdChainData();
    }

    protected void readXaForget(NetConnection conn) throws DisconnectException {
        startSameIdChainParse();
        parseSYNCCTLreply(conn);
        endOfSameIdChainData();
    }
    //----------------------helper methods----------------------------------------

    //--------------------- parse DDM Reply Data--------------------------------------


    // The SYNCCRD Reply Mesage
    //
    // Returned from Server:
    //   XARETVAL - required
    int parseSYNCCRD(ConnectionCallbackInterface connection) throws DisconnectException {
        boolean svrcodReceived = false;
        int svrcod = CodePoint.SVRCOD_INFO;
        int xaretval = 0;
        int synctype = 0;
        java.util.Hashtable indoubtTransactions = null;
        NetConnection conn = netAgent_.netConnection_;

        parseLengthAndMatchCodePoint(CodePoint.SYNCCRD);
        pushLengthOnCollectionStack();
        int peekCP = peekCodePoint();

        while (peekCP != Reply.END_OF_COLLECTION) {

            boolean foundInPass = false;

            if (peekCP == CodePoint.SVRCOD) {
                foundInPass = true;
                svrcodReceived = checkAndGetReceivedFlag(svrcodReceived);
                svrcod = parseSVRCOD(CodePoint.SVRCOD_ERROR, CodePoint.SVRCOD_ERROR);
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.XARETVAL) {
                foundInPass = true;
                xaretval = parseXARETVAL();
                conn.xares_.callInfoArray_[conn.currXACallInfoOffset_].xaRetVal_ =
                        xaretval;
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.SYNCTYPE) {
                foundInPass = true;
                synctype = parseSYNCTYPE();
                peekCP = peekCodePoint();
            }

            if (peekCP == CodePoint.PRPHRCLST) {
                foundInPass = true;
                indoubtTransactions = parseIndoubtList();
                conn.setIndoubtTransactions(indoubtTransactions);
                peekCP = peekCodePoint();
            }

            if (!foundInPass) {
                doPrmnsprmSemantics(peekCP);
            }
        }
        popCollectionStack();


        return xaretval;

    }

    // Process XA return value
    protected int parseXARETVAL() throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.XARETVAL);
        return readInt();
    }

    // Process XA return value
    protected byte parseSYNCTYPE() throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.SYNCTYPE);
        return readByte();
    }

    // This method handles the parsing of all command replies and reply data
    // for the SYNNCTL command.
    protected int parseSYNCCTLreply(ConnectionCallbackInterface connection) throws DisconnectException {
        int retval = 0;
        int peekCP = peekCodePoint();

        if (peekCP != CodePoint.SYNCCRD) {
            parseSYNCCTLError(peekCP);
            return -1;
        }
        retval = parseSYNCCRD(connection);

        peekCP = peekCodePoint();
        while (peekCP == CodePoint.SQLSTT) {
            String s = parseSQLSTT();
            //JCFTMP, need to null out the client list?
            netAgent_.netConnection_.xares_.addSpecialRegisters(s);
            peekCP = peekCodePoint();
        }
        if (peekCP == CodePoint.PBSD) {
            parsePBSD();
        }

        return retval;
    }


    //------------------------parse DDM Scalars-----------------------------




    private String parseSQLSTT() throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.SQLSTT);
        return parseSQLSTTGRPNOCMorNOCS();
    }

    private String parseSQLSTTGRPNOCMorNOCS() throws DisconnectException {
        int mixedNullInd = readUnsignedByte();
        int singleNullInd = 0;
        String sqlsttString = null;
        int stringLength = 0;

        if (mixedNullInd == CodePoint.NULLDATA) {
            singleNullInd = readUnsignedByte();
            if (singleNullInd == CodePoint.NULLDATA) {
                // throw DTAMCHRM
                doDtamchrmSemantics();
            }
            // read 4-byte length
            stringLength = readInt();
            // read sqlstt string
            sqlsttString = readString(stringLength, netAgent_.targetTypdef_.getCcsidSbcEncoding());
        } else {
            // read 4-byte length
            stringLength = readInt();
            // read sqlstt string
            sqlsttString = readString(stringLength, netAgent_.targetTypdef_.getCcsidMbcEncoding());
            // read null indicator
            readUnsignedByte();
        }
        return sqlsttString;
    }


    protected int parseXIDCNT() throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.XIDCNT);
        return readUnsignedShort();
    }

    protected Xid parseXID() throws DisconnectException {
        parseLengthAndMatchCodePoint(CodePoint.XID);
        int formatId = readInt();
        int gtridLen = readInt();
        int bqualLen = readInt();
        byte[] gtrid = readBytes(gtridLen);
        byte[] bqual = readBytes(bqualLen);

        return new com.splicemachine.db.client.ClientXid(formatId, gtrid, bqual);
    }

    protected java.util.Hashtable parseIndoubtList() throws DisconnectException {
        int port = 0;
        int numXid = 0;
        String sIpAddr = null;
        int peekCP = peekCodePoint();
        parseLengthAndMatchCodePoint(CodePoint.PRPHRCLST);
        peekCP = peekCodePoint();
        if (peekCP == CodePoint.XIDCNT) {
            parseXIDCNT();
            peekCP = peekCodePoint();
        }

        java.util.Hashtable indoubtTransactions = new java.util.Hashtable();
        while (peekCP == CodePoint.XID) {
            Xid xid = parseXID();
            indoubtTransactions.put(xid, new NetIndoubtTransaction(xid, null, null, null, sIpAddr, port));
            peekCP = peekCodePoint();
        }

        return indoubtTransactions;
    }

}



