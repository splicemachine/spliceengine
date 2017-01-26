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

package com.splicemachine.derby.ddl;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.ddl.DDLMessage;

import java.util.Collection;

/**
 * A Communication scheme for use in DDL.
 *
 * This primarily exists to abstract away ZooKeeper behavior, for easier testing (although it can also
 * be used to implement alternative communication schemes for DDL coordination).
 *
 * @author Scott Fines
 *         Date: 9/4/15
 */
public interface DDLCommunicator{


    String createChangeNode(DDLMessage.DDLChange change) throws StandardException;

    /**
     * @param asyncListener a listener to be notified of future events. Once the listener
     *                      is fired, it will not be fired again (thus the need to specify
     *                      the callback here on each call).
     * @return a list of the currently active ddl listeners.
     */
    Collection<String> activeListeners(com.splicemachine.derby.ddl.CommunicationListener asyncListener) throws StandardException;

    Collection<String> completedListeners(String changeId, com.splicemachine.derby.ddl.CommunicationListener asyncListener) throws StandardException;

    String getErrorMessage(String changeId, String errorId) throws StandardException;

    void deleteChangeNode(String changeId);
}
