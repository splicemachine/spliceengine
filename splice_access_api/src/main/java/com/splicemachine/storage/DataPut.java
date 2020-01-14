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
 * @author Scott Fines
 *         Date: 12/16/15
 */
public interface DataPut extends DataMutation{

    void tombstone(long txnIdLong);

    void antiTombstone(long txnIdLong);

    void addFirstWriteToken(byte[] family, long txnIdLong);

    void addDeleteRightAfterFirstWriteToken(byte[] family, long txnIdLong);

    void addCell(byte[] family, byte[] qualifier, long timestamp, byte[] value);

    void addCell(byte[] family, byte[] qualifier, byte[] value);

    byte[] key();

    Iterable<DataCell> cells();

    void addCell(DataCell kv);

    void skipWAL();
}
