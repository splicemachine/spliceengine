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

package com.splicemachine.pipeline.api;


/**
 * @author Scott Fines
 *         Date: 2/5/14
 */
public interface WriteStats {
		WriteStats NOOP_WRITE_STATS = new WriteStats() {

			@Override
			public long getWrittenCounter() {
				return 0;
			}

			@Override
			public long getRetryCounter() {
				return 0;
			}

			@Override
			public long getThrownErrorsRows() {
				return 0;
			}

			@Override
			public long getRetriedRows() {
				return 0;
			}

			@Override
			public long getPartialRows() {
				return 0;
			}

			@Override
			public long getPartialThrownErrorRows() {
				return 0;
			}

			@Override
			public long getPartialRetriedRows() {
				return 0;
			}

			@Override
			public long getPartialIgnoredRows() {
				return 0;
			}

			@Override
			public long getPartialWrite() {
				return 0;
			}

			@Override
			public long getIgnoredRows() {
				return 0;
			}

			@Override
			public long getCatchThrownRows() {
				return 0;
			}

			@Override
			public long getCatchRetriedRows() {
				return 0;
			}

			@Override
			public long getRegionTooBusy() {
				return 0;
			}
		};

	long getWrittenCounter();
	long getRetryCounter();
	long getThrownErrorsRows();
	long getRetriedRows();
	long getPartialRows();
	long getPartialThrownErrorRows();
	long getPartialRetriedRows();
	long getPartialIgnoredRows();
	long getPartialWrite();
	long getIgnoredRows();
	long getCatchThrownRows();
	long getCatchRetriedRows();
	long getRegionTooBusy();
}
