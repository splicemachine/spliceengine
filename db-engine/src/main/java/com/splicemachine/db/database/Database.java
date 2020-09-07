/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
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
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.database;

/*
  The com.splicemachine.db.iapi.db.Database interface is all the externally
  available methods on a database.  These are methods that might be called from 
  an SQL-J CALL statement. 

  The Javadoc comment that follows is for external consumption.
*/


import com.splicemachine.db.catalog.UUID;

import java.sql.SQLException;
import java.util.Locale;

/**
 * The Database interface provides control over a database
 * (that is, the stored data and the files the data are stored in),
 * operations on the database such as  backup and recovery,
 * and all other things that are associated with the database itself.
 * 
 *  @see com.splicemachine.db.iapi.db.Factory
 */
public interface Database
{

    /**
     * Tells whether the Database is configured as read-only, or the
     * Database was started in read-only mode.
     *
     * @return    TRUE means the Database is read-only, FALSE means it is
     *        not read-only.
     */
    boolean        isReadOnly();

    /**
     * Backup the database to a backup directory.  See online documentation
     * for more detail about how to use this feature.
     *
     * @param backupDir the directory name where the database backup should
     *         go.  This directory will be created if not it does not exist.
     * @param wait if <tt>true</tt>, waits for  all the backup blocking 
     *             operations in progress to finish.
     * @exception SQLException Thrown on error
     */
    void backup(String backupDir, boolean wait)
        throws SQLException;

    void restore(String restoreDir, boolean wait)
            throws SQLException;

    /**
      * Freeze the database temporarily so a backup can be taken.
      * <P>Please see the Derby documentation on backup and restore.
      *
      * @exception SQLException Thrown on error
      */
    void freeze() throws SQLException;

    /**
      * Unfreeze the database after a backup has been taken.
      * <P>Please see the Derby documentation on backup and restore.
      *
      * @exception SQLException Thrown on error
      */
    void unfreeze() throws SQLException;

    /**
     * Checkpoints the database, that is, flushes all dirty data to disk.
     * Records a checkpoint in the transaction log, if there is a log.
     *
     * @exception SQLException Thrown on error
     */
    void checkpoint() throws SQLException;

    /**
     * Get the Locale for this database.
     */
    Locale getLocale();

    /**
        Return the UUID of this database.
        @deprecated No longer supported.

    */
    UUID getId();
}



