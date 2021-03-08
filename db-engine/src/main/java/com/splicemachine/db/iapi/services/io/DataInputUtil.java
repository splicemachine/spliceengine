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

package com.splicemachine.db.iapi.services.io;

import com.splicemachine.db.impl.sql.catalog.BaseDataDictionary;

import java.io.DataInput;
import java.io.IOException;

/**
 * A util class for DataInput.
 */
public final class DataInputUtil {

    /**
     * Skips requested number of bytes,
     * throws EOFException if there is too few bytes in the DataInput.
     * @param in
     *      DataInput to be skipped.
     * @param skippedBytes
     *      number of bytes to skip. if skippedBytes <= zero, do nothing.
     * @throws IOException
     *      if IOException occurs. It doesn't contain EOFException.
     * @throws NullPointerException
     *      if the param 'in' equals null.
     */
    public static void skipFully(DataInput in, int skippedBytes)
    throws IOException {
        if (in == null) {
            throw new NullPointerException();
        }

        while (skippedBytes > 0) {
            int skipped = in.skipBytes(skippedBytes);
            if (skipped == 0) {
                in.readByte();
                skipped++;
            }
            skippedBytes -= skipped;
        }
    }

    /**
     * The objects stored in data dictionary are serialized in old format if
     * 1) BaseDataDictionary.SPLICE_CATALOG_SERIALIZATION_VERSION < 2 and
     * 2) BaseDataDictionary.SPLICE_CATALOG_SERIALIZATION_VERSION != Integer.MIN_VALUE
     *
     * A client should always see objects in newer format after this commit, because all objects stored in dictionary
     * are upgraded. The server may see objects in old format before/during upgrade.
     *
     * For server, BaseDataDictionary.SPLICE_CATALOG_SERIALIZATION_VERSION is -1 before upgrade, and set to 2 after
     * upgrade.
     *
     * For clients, BaseDataDictionary.SPLICE_CATALOG_SERIALIZATION_VERSION is always Integer.MIN_VALUE
     * @return
     */
    public static boolean shouldReadOldFormat() {
        return !BaseDataDictionary.READ_NEW_FORMAT;
    }

    public static boolean shouldWriteOldFormat() {
        return !BaseDataDictionary.WRITE_NEW_FORMAT;
    }
}
