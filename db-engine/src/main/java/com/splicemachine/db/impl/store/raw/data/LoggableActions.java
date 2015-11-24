/*

   Derby - Class com.splicemachine.db.impl.store.raw.data.LoggableActions

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package com.splicemachine.db.impl.store.raw.data;

import com.splicemachine.db.iapi.store.access.conglomerate.LogicalUndo;

import com.splicemachine.db.iapi.store.raw.xact.RawTransaction;
import com.splicemachine.db.iapi.store.raw.RecordHandle;
import com.splicemachine.db.iapi.store.raw.Loggable;
import com.splicemachine.db.iapi.store.raw.log.LogInstant;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.io.DynamicByteArrayOutputStream;


public class LoggableActions implements PageActions  {


	public void actionDelete(
    RawTransaction  t, 
    BasePage        page, 
    int             slot, 
    int             recordId, 
    boolean         delete, 
    LogicalUndo     undo)
		throws StandardException
	{

		DeleteOperation lop = 
            new DeleteOperation(t, page, slot, recordId, delete, undo);

		doAction(t, page, lop);
	}

	public int actionUpdate(
    RawTransaction          t, 
    BasePage                page, 
    int                     slot, 
    int                     recordId,
    Object[]   row, 
    FormatableBitSet                 validColumns,
    int                     realStartColumn, 
    DynamicByteArrayOutputStream  logBuffer, 
    int                     realSpaceOnPage, 
    RecordHandle            headRowHandle)
		throws StandardException
	{
		UpdateOperation lop = 
			new UpdateOperation(t, page, slot, recordId, row, validColumns,
								realStartColumn, logBuffer,
								realSpaceOnPage, headRowHandle);

		doAction(t, page, lop);

		return lop.getNextStartColumn();
	}

	public void actionPurge(
    RawTransaction  t, 
    BasePage        page, 
    int             slot, 
    int             num_rows, 
    int[]           recordIds,
	boolean         logData)
		throws StandardException
	{
		PurgeOperation lop = 
            new PurgeOperation(t, page, slot, num_rows, recordIds, logData);

		doAction(t, page, lop);
	}

	public void actionUpdateField(
    RawTransaction          t, 
    BasePage                page, 
    int                     slot, 
    int                     recordId, 
    int                     fieldId,
    Object     newValue, 
    LogicalUndo             undo)
		throws StandardException
	{
		UpdateFieldOperation lop = 
            new UpdateFieldOperation(
                    t, page, slot, recordId, fieldId, newValue, undo);

		doAction(t, page, lop);
	}

	public int actionInsert(
    RawTransaction          t, 
    BasePage                page, 
    int                     slot, 
    int                     recordId,
    Object[]                row, 
    FormatableBitSet                 validColumns, 
    LogicalUndo             undo, 
    byte                    insertFlag,
    int                     startColumn, 
    boolean                 isLongColumn, 
    int                     realStartColumn, 
    DynamicByteArrayOutputStream  logBuffer,
    int                     realSpaceOnPage, 
    int                     overflowThreshold)
		throws StandardException
	{
		InsertOperation lop = new InsertOperation(t, page, slot, recordId,
			row, validColumns, undo, insertFlag, startColumn, isLongColumn,
			realStartColumn, logBuffer, realSpaceOnPage, overflowThreshold);

		doAction(t, page, lop);
		return (lop.getNextStartColumn());

	}

	public void actionCopyRows(
    RawTransaction  t, 
    BasePage        destPage, 
    BasePage        srcPage,
    int             srcSlot, 
    int             numRows, 
    int             destSlot, 
    int[]           recordIds)
		throws StandardException
	{

		CopyRowsOperation lop = 
            new CopyRowsOperation(
                    t, destPage, srcPage,srcSlot, numRows, destSlot, recordIds);

		doAction(t, destPage, lop);
	}

	public void actionInvalidatePage(RawTransaction t, BasePage page)
		 throws StandardException
	{
		
		InvalidatePageOperation lop = new InvalidatePageOperation(page);
		doAction(t, page, lop);
	}

	public void actionInitPage(
    RawTransaction  t, 
    BasePage        page, 
    int             initFlag,
    int             pageFormatId, 
    long            pageOffset)
		 throws StandardException
	{
		InitPageOperation lop = 
			new InitPageOperation(page, initFlag, pageFormatId, pageOffset);

		doAction(t, page, lop);
	}

	public void actionShrinkReservedSpace(
    RawTransaction  t, 
    BasePage        page, 
    int             slot, 
    int             recordId, 
    int             newValue, 
    int             oldValue)
		 throws StandardException
	{

		SetReservedSpaceOperation lop = 
			new SetReservedSpaceOperation(
                    page, slot, recordId, newValue, oldValue);

		doAction(t, page, lop);
	}

	private void doAction(RawTransaction t, BasePage page, Loggable lop)
		 throws StandardException
	{
		long oldversion = 0;		// sanity check
		LogInstant oldLogInstant = null; // sanity check
		if (SanityManager.DEBUG)
		{
			oldLogInstant = page.getLastLogInstant();
			oldversion = page.getPageVersion();
		}

		// mark the page as pre-dirtied so that if a checkpoint happens after
		// the log record is sent to the log stream, the cache cleaning will
		// wait for this change.
		page.preDirty();

		t.logAndDo(lop);

		if (SanityManager.DEBUG) {
			// log instant may not supported by underlying log factory, in that
			// case, it is expected to stay null
			if (oldLogInstant != null &&  page.getLastLogInstant() != null &&
							! oldLogInstant.lessThan(page.getLastLogInstant()))
				SanityManager.THROWASSERT(
								 "old log instant = " + oldLogInstant + 
								 " lastlog = " + page.getLastLogInstant() );

			SanityManager.ASSERT(
                    oldversion == ((PageBasicOperation)lop).getPageVersion());
			SanityManager.ASSERT(page.getPageVersion() > oldversion);
		}

	}

}
