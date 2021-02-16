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

package com.splicemachine.db.impl.sql.execute;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import com.splicemachine.db.iapi.sql.dictionary.*;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;


/**
 * An extension to TriggerInfo so triggers serialized on disk
 * or in jar files will still work.
 */
@SuppressFBWarnings("EI_EXPOSE_REP")
public class TriggerInfo2 extends TriggerInfo {

    private String tableName;
    private static int versionNumber = 2;

    /**
     * Default constructor for Formattable
     */
    public TriggerInfo2() {
    }

    public TriggerInfo2(TableDescriptor td, int[] changedCols, GenericDescriptorList<TriggerDescriptor> triggers) {
        super(td, changedCols, triggers);
        if (td != null)
            this.tableName = td.getQualifiedName();
    }

    public String getTableName() {
        return tableName;
    }


    @Override
    public String toString() {
        return super.toString() + ", targetTableName=" + tableName;
    }

    /**
     * Write this object out
     *
     * @param out write bytes here
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        // DO NOT CHANGE THIS SERIALIZATION
        super.writeExternal(out);
        out.writeInt(versionNumber);
        writeNullableUTF(out, tableName);
    }

    /**
     * Read this object from a stream of stored objects.
     *
     * @param in read this.
     */
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // DO NOT CHANGE THIS SERIALIZATION
        super.readExternal(in);
        in.readInt();
        tableName = readNullableUTF(in);
    }

    public static void writeNullableUTF(ObjectOutput out, String str) throws IOException {
        out.writeBoolean(str != null);
        if (str != null)
            out.writeUTF(str);
    }

    public static String readNullableUTF(ObjectInput in) throws IOException {
        if (in.readBoolean()) {
            return in.readUTF();
        }
        else {
            return null;
        }
    }
}
