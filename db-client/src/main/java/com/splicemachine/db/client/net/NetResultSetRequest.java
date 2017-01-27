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
 * All Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
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

import com.splicemachine.db.client.am.ColumnMetaData;
import com.splicemachine.db.client.am.ResultSet;
import com.splicemachine.db.client.am.Section;
import com.splicemachine.db.client.am.SqlException;
import com.splicemachine.db.client.am.ClientMessageId;

import com.splicemachine.db.shared.common.reference.SQLState;

public class NetResultSetRequest extends NetStatementRequest
        implements ResultSetRequestInterface {
    public NetResultSetRequest(NetAgent netAgent, int bufferSize) {
        super(netAgent, bufferSize);
    }

    //----------------------------- entry points ---------------------------------
    public void writeFetch(NetResultSet resultSet,
                           Section section,
                           int fetchSize) throws SqlException {
        // - for forward-only cursors we do not send qryrowset on OPNQRY, fetchSize is ignored.
        //   but qryrowset is sent on EXCSQLSTT for a stored procedure call.
        boolean sendQryrowset =
                ((NetStatement) resultSet.statement_.materialStatement_).qryrowsetSentOnOpnqry_;

        boolean sendRtnextdta = false;
        if (sendQryrowset && resultSet.resultSetType_ == java.sql.ResultSet.TYPE_FORWARD_ONLY &&
                ((NetCursor) resultSet.cursor_).hasLobs_) {
            fetchSize = 1;
            resultSet.fetchSize_ = 1;
            sendRtnextdta = true;
            ((NetCursor) resultSet.cursor_).rtnextrow_ = false;
        }
        // if one of the result sets returned from a stored procedure is scrollable,
        // then we set netStatement_.qryrowsetSentOnOpnqry_ to true even though we didn't really
        // send a qryrowset on excsqlstt for sqlam >= 7.  this is ok for scrollable cursors,
        // but will cause a problem for forward-only cursors.  Because if fetchSize was never
        // set, we will send qryrowset(0), which will cause a syntaxrm.
        else if (resultSet.fetchSize_ == 0) {
            sendQryrowset = false;
        }

        buildCNTQRY(section,
                sendQryrowset,
                resultSet.queryInstanceIdentifier_,
                fetchSize,
                sendRtnextdta);

        buildOUTOVR(resultSet,
                resultSet.resultSetMetaData_,
                resultSet.firstOutovrBuilt_,
                ((NetCursor) resultSet.cursor_).hasLobs_);
    }

    public void writeScrollableFetch(NetResultSet resultSet,
                                     Section section,
                                     int fetchSize,
                                     int orientation,
                                     long rowToFetch,
                                     boolean resetQueryBlocks) throws SqlException {
        int protocolOrientation = computePROTOCOLOrientation(orientation);

        // - for sensitive-static cursors:
        //     * qryrowset must be sent on opnqry to indicate to the server that the cursor is
        //       going to be used in a scrollable fashion.  (sqlam<7)
        //     * if qryrowset is sent on opnqry, then it must be sent on all subsequent cntqry's
        // - for sensitive-dynamic non-rowset cursors: (we should never be in this case)
        //     * qryrowset is NOT ALLOWED on cntqry's
        // - for rowset cursors:
        //     * qryrowset is optional.  it is ignored on opnqry.  if not sent on cntqry,
        //       then the fetch is going fetch next row as opposed to fetch next rowset.
        boolean sendQryrowset =
                (resultSet.isRowsetCursor_ ||
                (((NetStatement) resultSet.statement_.materialStatement_).qryrowsetSentOnOpnqry_ &&
                (resultSet.sensitivity_ == ResultSet.sensitivity_sensitive_static__ ||
                ((NetCursor) resultSet.cursor_).blocking_)));

        buildScrollCNTQRY(protocolOrientation,
                rowToFetch,
                section,
                sendQryrowset,
                resultSet.queryInstanceIdentifier_,
                fetchSize,
                resetQueryBlocks);

        buildOUTOVR(resultSet,
                resultSet.resultSetMetaData_,
                resultSet.firstOutovrBuilt_,
                ((NetCursor) resultSet.cursor_).hasLobs_);
    }

    public void writePositioningFetch(NetResultSet resultSet,
                                      Section section,
                                      int orientation,
                                      long rowToFetch) throws SqlException {
        int protocolOrientation = computePROTOCOLOrientation(orientation);

        // do not send qryrowste if the cursor is a non-rowset, sensitive dynamic cursor
        boolean sendQryrowset =
                resultSet.isRowsetCursor_ ||
                (((NetStatement) resultSet.statement_.materialStatement_).qryrowsetSentOnOpnqry_ &&
                resultSet.sensitivity_ != resultSet.sensitivity_sensitive_dynamic__);

        buildPositioningCNTQRY(protocolOrientation,
                rowToFetch,
                section,
                sendQryrowset,
                resultSet.queryInstanceIdentifier_,
                resultSet.fetchSize_);

        buildOUTOVR(resultSet,
                resultSet.resultSetMetaData_,
                resultSet.firstOutovrBuilt_,
                ((NetCursor) resultSet.cursor_).hasLobs_);
    }

    public void writeCursorClose(NetResultSet resultSet,
                                 Section section) throws SqlException {
        buildCLSQRY(section,
                resultSet.queryInstanceIdentifier_);
    }

    //----------------------helper methods----------------------------------------
    // These methods are "private protected", which is not a recognized java privilege,
    // but means that these methods are private to this class and to subclasses,
    // and should not be used as package-wide friendly methods.

    private void buildCLSQRY(Section section,
                             long queryInstanceIdentifier)
            throws SqlException {
        createCommand();
        markLengthBytes(CodePoint.CLSQRY);
        buildPKGNAMCSN(section);
        buildQRYINSID(queryInstanceIdentifier);
        updateLengthBytes();
    }

    private void buildCNTQRY(Section section,
                             boolean sendQryrowset,
                             long queryInstanceIdentifier,
                             int qryrowsetSize,
                             boolean sendRtnextdta) throws SqlException {
        buildCoreCNTQRY(section,
                sendQryrowset,
                queryInstanceIdentifier,
                qryrowsetSize);

        // We will always let RTNEXTDTA default to RTNEXTROW.  The only time we need to send
        // RTNEXTDTA RTNEXTALL is for a stored procedure returned forward-only ResultSet
        // that has LOB columns.  Since there are LOBs in the
        // ResultSet, no QRYDTA is returned on execute.  On the CNTQRY's, we will
        // send qryrowset(1) and rtnextall.
        if (sendRtnextdta) {
            buildRTNEXTDTA(CodePoint.RTNEXTALL);
        }


        updateLengthBytes();
    }

    // buildCoreCntqry builds the common parameters
    private void buildCoreCNTQRY(Section section,
                                 boolean sendQryrowset,
                                 long queryInstanceIdentifier,
                                 int qryrowsetSize)
            throws SqlException {
        createCommand();
        markLengthBytes(CodePoint.CNTQRY);

        buildPKGNAMCSN(section); // 1. packageNameAndConsistencyToken
        buildQRYBLKSZ(); // 2. qryblksz

        // maxblkext (-1) tells the server that the client is capable of receiving any number of query blocks
        if (sendQryrowset) {
            buildMAXBLKEXT(-1); // 3. maxblkext
        }

        // 4. qryinsid
        buildQRYINSID(queryInstanceIdentifier);

        if (sendQryrowset) {
            buildQRYROWSET(qryrowsetSize);  // 5. qryrowset
        }
    }

    // Send CNTQRY to get a new rowset from the target server.
    private void buildScrollCNTQRY(int scrollOrientation,
                                   long rowNumber,
                                   Section section,
                                   boolean sendQryrowset,
                                   long queryInstanceIdentifier,
                                   int qryrowsetSize,
                                   boolean resetQueryBlocks)
            throws SqlException {
        buildCoreCNTQRY(section,
                sendQryrowset,
                queryInstanceIdentifier,
                qryrowsetSize);

        buildQRYSCRORN(scrollOrientation); // qryscrorn

        if (scrollOrientation == CodePoint.QRYSCRABS || scrollOrientation == CodePoint.QRYSCRREL) {
            buildQRYROWNBR(rowNumber);
        }

        if (resetQueryBlocks) {
            buildQRYBLKRST(0xF1);  // do reset the rowset
        } else {
            buildQRYBLKRST(0xF0);  // do not reset the rowset
        }

        buildQRYRTNDTA(0xF1);    // do return data

        updateLengthBytes();
    }

    // Send CTNQRY to reposition the cursor on the target server.
    private void buildPositioningCNTQRY(int scrollOrientation,
                                        long rowNumber,
                                        Section section,
                                        boolean sendQryrowset,
                                        long queryInstanceIdentifier,
                                        int qryrowsetSize)
            throws SqlException {
        createCommand();
        markLengthBytes(CodePoint.CNTQRY);

        buildPKGNAMCSN(section); // 1. pkgnamcsn
        buildQRYBLKSZ(); // 2. qryblksz

        buildQRYINSID(queryInstanceIdentifier); // 3. qryinsid

        if (sendQryrowset) {
            buildQRYROWSET(qryrowsetSize);   // 4. qryrowset
        }

        buildQRYSCRORN(scrollOrientation); // 5. qryscrorn

        if (scrollOrientation == CodePoint.QRYSCRABS || scrollOrientation == CodePoint.QRYSCRREL) {
            buildQRYROWNBR(rowNumber); // 6. qryrownbr
        }

        buildQRYBLKRST(0xF1); // 7. do reset the rowset
        buildQRYRTNDTA(0xF0); // 8. do not return data


        updateLengthBytes(); // for cntqry
    }

    private void buildOUTOVR(NetResultSet resultSet,
                             ColumnMetaData resultSetMetaData,
                             boolean firstOutovrBuilt,
                             boolean hasLobs) throws SqlException {
        if (hasLobs) {
            if (!firstOutovrBuilt) {
                buildOUTOVR(resultSet, resultSetMetaData);
                resultSet.firstOutovrBuilt_ = true;
            }
        }
    }

    private void buildRTNEXTDTA(int rtnextdta) throws SqlException {
        writeScalar1Byte(CodePoint.RTNEXTDTA, rtnextdta);
    }

    private void buildQRYSCRORN(int scrollOrientation) throws SqlException {
        writeScalar1Byte(CodePoint.QRYSCRORN, scrollOrientation);
    }

    private void buildQRYBLKRST(int qryblkrst) throws SqlException {
        writeScalar1Byte(CodePoint.QRYBLKRST, qryblkrst);
    }

    private void buildQRYROWNBR(long rowNumber) throws SqlException {
        writeScalar8Bytes(CodePoint.QRYROWNBR, rowNumber);
    }

    private void buildQRYRTNDTA(int qryrtndta) throws SqlException {
        writeScalar1Byte(CodePoint.QRYRTNDTA, qryrtndta);
    }

    //----------------------non-parsing computational helper methods--------------
    // These methods are "private protected", which is not a recognized java privilege,
    // but means that these methods are private to this class and to subclasses,
    // and should not be used as package-wide friendly methods.

    // Called by NetResultSetRequest.writeScrollableFetch()
    private int computePROTOCOLOrientation(int orientation) throws SqlException {
        switch (orientation) {
        case ResultSet.scrollOrientation_absolute__:
            return CodePoint.QRYSCRABS;

        case ResultSet.scrollOrientation_after__:
            return CodePoint.QRYSCRAFT;

        case ResultSet.scrollOrientation_before__:
            return CodePoint.QRYSCRBEF;

        case ResultSet.scrollOrientation_relative__:
            return CodePoint.QRYSCRREL;

        default:
            throw new SqlException(netAgent_.logWriter_, 
                new ClientMessageId(SQLState.NET_INVALID_SCROLL_ORIENTATION));
        }
    }

}
