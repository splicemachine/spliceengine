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
package com.splicemachine.db.catalog.types;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.splicemachine.db.catalog.TypeDescriptor;
import com.splicemachine.db.iapi.services.io.FormatIdUtil;
import com.splicemachine.db.iapi.services.io.Formatable;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.sanity.SanityManager;

/**
 * Class to simply read the old format written by
 * DataTypeDescriptor prior to DERBY-2775 being addressed.
 * The format was incorrect used
 * in system catalogs for routine parameter and return
 * types. The format contained repeated information.
 * DERBY-2775 changed the code so that these catalog
 * types were written as TypeDescriptor (which is what
 * always had occurred for the types in SYSCOLUMNS).
 */
final class OldRoutineType implements Formatable {
    
    private TypeDescriptor catalogType;

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        
        // Redundant old TypeId object, just ignore,
        // comprised of two parts the old wrapper format number
        // and then a BaseTypeId. Information was duplicated
        // in the catalog type.
        FormatIdUtil.readFormatIdInteger(in);
        in.readObject(); 
        catalogType = (TypeDescriptor) in.readObject();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        if (SanityManager.DEBUG)
        {
            SanityManager.THROWASSERT("OldRoutineType must be read only!");
        }
    }

    public int getTypeFormatId() {
        return StoredFormatIds.DATA_TYPE_IMPL_DESCRIPTOR_V01_ID;
    }

    TypeDescriptor getCatalogType() {
        return catalogType;
    }
}
