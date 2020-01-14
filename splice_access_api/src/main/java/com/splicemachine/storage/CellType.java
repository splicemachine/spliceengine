/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.storage;

/**
 * Considering key-values from data tables, based on the column family and qualifier they can be classified as one of
 * these types.
 */
public enum CellType{

    /* Column "0" */
    COMMIT_TIMESTAMP,

    /* Column "1" (if empty value) */
    TOMBSTONE,

    /* Column "1" (if "0" value) */
    ANTI_TOMBSTONE,

    /* Column "7" */
    USER_DATA,

    /* Column "8" (if empty value) */
    FIRST_WRITE_TOKEN,

    /* Column "8" (if "0" value) */
    DELETE_RIGHT_AFTER_FIRST_WRITE_TOKEN,

    /* Column "9" */
    FOREIGN_KEY_COUNTER,

    /* Unrecognized column/column-value. */
    OTHER
}
