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

package com.splicemachine.db.impl.sql.catalog;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.DataValueFactory;
import com.splicemachine.db.iapi.types.SQLBlob;
import com.splicemachine.db.iapi.types.SQLTimestamp;
import com.splicemachine.db.iapi.types.SQLVarchar;
import org.joda.time.DateTime;

import java.sql.Blob;
import java.sql.Types;

public class SYSSOURCECODERowFactory extends CatalogRowFactory {
    private static final String TABLENAME_STRING = "SYSSOURCECODE";
    private static final int SOURCECODE_COLUMN_COUNT = 7;

    public static final int SCHEMA_NAME = 1;
    public static final int OBJECT_NAME = 2;
    public static final int OBJECT_TYPE = 3;
    public static final int OBJECT_FORM = 4;
    public static final int DEFINER_NAME = 5;
    public static final int LAST_MODIFIED = 6;
    public static final int SOURCE_CODE = 7;

    static final int SYSSOURCECODE_INDEX1_ID = 0;

    private static final int[][] indexColumnPositions = {
        {SCHEMA_NAME, OBJECT_NAME, OBJECT_TYPE, OBJECT_FORM},
    };

    private static final boolean[] uniqueness = {true};

    private static String uuids[] = {
        "f3779fc7-7e3d-4138-8bc5-fc10c3834250", // catalog UUID
        "f3779fc7-7e3d-4138-8bc5-fc10c3834251", // heap UUID
        "f3779fc7-7e3d-4138-8bc5-fc10c3834252"  // index1
    };

    public SYSSOURCECODERowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf, DataDictionary dd) {
        super(uuidf, ef, dvf, dd);
        initInfo(SOURCECODE_COLUMN_COUNT, TABLENAME_STRING, indexColumnPositions, uniqueness, uuids);
    }

    @Override
    public ExecRow makeRow(boolean latestVersion, TupleDescriptor td, TupleDescriptor parent) throws StandardException {

        String schemaName = null;
        String objectName = null;
        String objectType = null;
        String objectForm = null;
        String definerName = null;
        DateTime lastModified = null;
        Blob sourceCode = null;

        if (td != null) {
            if (!(td instanceof SourceCodeDescriptor))
                throw new RuntimeException("Unexpected TupleDescriptor " + td.getClass().getName());

            SourceCodeDescriptor d = (SourceCodeDescriptor)td;
            schemaName = d.getSchemaName();
            objectName = d.getObjectName();
            objectType = d.getObjectType();
            objectForm = d.getObjectForm();
            definerName = d.getDefinerName();
            lastModified = d.getLastModified();
            sourceCode = d.getSourceCode();
        }

        ExecRow row = getExecutionFactory().getValueRow(SOURCECODE_COLUMN_COUNT);

        row.setColumn(SCHEMA_NAME, new SQLVarchar(schemaName));
        row.setColumn(OBJECT_NAME, new SQLVarchar(objectName));
        row.setColumn(OBJECT_TYPE, new SQLVarchar(objectType));
        row.setColumn(OBJECT_FORM, new SQLVarchar(objectForm));
        row.setColumn(DEFINER_NAME, new SQLVarchar(definerName));
        row.setColumn(LAST_MODIFIED, new SQLTimestamp(lastModified));
        row.setColumn(SOURCE_CODE, new SQLBlob(sourceCode));

        return row;
    }

    @Override
    public TupleDescriptor buildDescriptor(ExecRow row,
                                           TupleDescriptor parentTuple,
                                           DataDictionary dataDictionary) throws StandardException {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(
                    row.nColumns() == SOURCECODE_COLUMN_COUNT,
                    "Wrong number of columns for a SOURCECODE row");
        }

        DataValueDescriptor col = row.getColumn(SCHEMA_NAME);
        String schemaName = col.getString();

        col = row.getColumn(OBJECT_NAME);
        String objectName = col.getString();

        col = row.getColumn(OBJECT_TYPE);
        String objectType = col.getString();

        col = row.getColumn(OBJECT_FORM);
        String objectForm = col.getString();

        col = row.getColumn(DEFINER_NAME);
        String definerName = col.getString();

        col = row.getColumn(LAST_MODIFIED);
        DateTime lastModified = col.getDateTime();

        col = row.getColumn(SOURCE_CODE);
        Blob sourceCode = (Blob)col.getObject();

        return new SourceCodeDescriptor(schemaName, objectName, objectType, objectForm, definerName, lastModified, sourceCode);
    }

    @Override
    public SystemColumn[] buildColumnList() throws StandardException {
        return new SystemColumn[]{
                SystemColumnImpl.getColumn("SCHEMA_NAME", Types.VARCHAR, false, 128),
                SystemColumnImpl.getColumn("OBJECT_NAME", Types.VARCHAR, false, 128),
                SystemColumnImpl.getColumn("OBJECT_TYPE", Types.VARCHAR, false, 32),
                SystemColumnImpl.getColumn("OBJECT_FORM", Types.VARCHAR, false, 32),
                SystemColumnImpl.getColumn("DEFINER_NAME",Types.VARCHAR, false, 128),
                SystemColumnImpl.getColumn("LAST_MODIFIED",Types.TIMESTAMP, false, 0),
                SystemColumnImpl.getColumn("SOURCE_CODE", Types.BLOB, false, 64*1024*1024)
        };
    }
}
