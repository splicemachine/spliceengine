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

package com.splicemachine.derby.utils;

import javax.management.MXBean;
import java.util.List;
import java.util.Map;

/**
 * @author Scott Fines
 * Created on: 8/15/13
 */
@MXBean
public interface ErrorReport {

    public List<String> getRecentThrowingClassNames();

    public List<String> getRecentReportingClassNames();

    public Map<String,Long> getMostRecentErrors();

    public long getTotalErrors();

    public long getTotalIOExceptions();

    public long getTotalDoNotRetryIOExceptions();

    public long getTotalStandardExceptions();

    public long getTotalExecutionExceptions();

    public long getTotalRuntimeExceptions();
}
