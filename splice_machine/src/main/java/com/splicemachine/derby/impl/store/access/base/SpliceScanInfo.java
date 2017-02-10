/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.store.access.base;

import com.splicemachine.db.iapi.store.access.ScanInfo;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.i18n.MessageService;

import java.util.Properties;

/**

  This object provides performance information related to an open scan.
  The information is accumulated during operations on a ScanController() and
  then copied into this object and returned by a call to 
  ScanController.getStatistic().

  @see  com.splicemachine.db.iapi.store.access.ScanController#getScanInfo()

**/
public class SpliceScanInfo implements ScanInfo {
    /**
     * Performance counters ...
     */
	private int     stat_numpages_visited       = 0;
    private int     stat_numrows_visited        = 0;
    private int     stat_numrows_qualified      = 0;
    private int     stat_numColumnsFetched      = 0;
    private FormatableBitSet  stat_validColumns           = null;

    /* Constructors for This class: */
    public SpliceScanInfo(SpliceScan scan) {
        // copy perfomance state out of scan, to get a fixed set of stats
        /*stat_numpages_visited       = scan.getNumPagesVisited();
        stat_numrows_visited        = scan.getNumRowsVisited();
        stat_numrows_qualified      = scan.getNumRowsQualified();

        stat_validColumns = 
            (scan.getScanColumnList() == null ? 
                null : ((FormatableBitSet) scan.getScanColumnList().clone()));

        if (stat_validColumns == null)
        {
            stat_numColumnsFetched = ((HBaseConglomerate) scan.getOpenConglom().getConglomerate()).format_ids.length;
        }
        else
        {
            for (int i = 0; i < stat_validColumns.size(); i++)
            {
                if (stat_validColumns.get(i))
                    stat_numColumnsFetched++;
            }
        }
		*/
    }

    /**
     * Return all information gathered about the scan.
     * <p>
     * This routine returns a list of properties which contains all information
     * gathered about the scan.  If a Property is passed in, then that property
     * list is appeneded to, otherwise a new property object is created and
     * returned.
     * <p>
     * Not all scans may support all properties, if the property is not 
     * supported then it will not be returned.  The following is a list of
     * properties that may be returned:
     *
     *     numPagesVisited
     *         - the number of pages visited during the scan.  For btree scans
     *           this number only includes the leaf pages visited.  
     *     numRowsVisited
     *         - the number of rows visited during the scan.  This number 
     *           includes all rows, including: those marked deleted, those
     *           that don't meet qualification, ...
     *     numRowsQualified
     *         - the number of undeleted rows, which met the qualification.
     *     treeHeight (btree's only)
     *         - for btree's the height of the tree.  A tree with one page
     *           has a height of 1.  Total number of pages visited in a btree
     *           scan is (treeHeight - 1 + numPagesVisited).
     *     NOTE - this list will be expanded as more information about the scan
     *            is gathered and returned.
     *
     * @param prop   Property list to fill in.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public Properties getAllScanInfo(Properties prop)
		throws StandardException
    {
        if (prop == null)
            prop = new Properties();

        prop.put(
			MessageService.getTextMessage(SQLState.STORE_RTS_SCAN_TYPE),
			MessageService.getTextMessage(SQLState.STORE_RTS_HEAP));
        prop.put(
			MessageService.getTextMessage(SQLState.STORE_RTS_NUM_PAGES_VISITED),
            Integer.toString(stat_numpages_visited));
        prop.put(
			MessageService.getTextMessage(SQLState.STORE_RTS_NUM_ROWS_VISITED),
            Integer.toString(stat_numrows_visited));
        prop.put(
		  MessageService.getTextMessage(SQLState.STORE_RTS_NUM_ROWS_QUALIFIED),
          Integer.toString(stat_numrows_qualified));
        prop.put(
		  MessageService.getTextMessage(SQLState.STORE_RTS_NUM_COLUMNS_FETCHED),
          Integer.toString(stat_numColumnsFetched));
        prop.put(
	  MessageService.getTextMessage(SQLState.STORE_RTS_COLUMNS_FETCHED_BIT_SET),
			(stat_validColumns == null ?
				MessageService.getTextMessage(SQLState.STORE_RTS_ALL) :
                stat_validColumns.toString()));

        return(prop);
    }
}
