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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.catalog;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.types.SQLChar;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.SystemColumn;
import com.splicemachine.db.iapi.types.DataValueFactory;
import com.splicemachine.db.iapi.sql.dictionary.CatalogRowFactory;
import com.splicemachine.db.iapi.sql.dictionary.DataDescriptorGenerator;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.RoleGrantDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TupleDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.services.sanity.SanityManager;

/**
 * Factory for creating a SYSROLES row.
 */

public class SYSROLESRowFactory extends CatalogRowFactory
{
    private static final String TABLENAME_STRING = "SYSROLES";

    private static final int SYSROLES_COLUMN_COUNT = 6;
    /* Column #s for sysinfo (1 based) */
    private static final int SYSROLES_ROLE_UUID = 1;
    private static final int SYSROLES_ROLEID = 2;
    private static final int SYSROLES_GRANTEE = 3;
    private static final int SYSROLES_GRANTOR = 4;
    private static final int SYSROLES_WITHADMINOPTION = 5;
    static final int SYSROLES_ISDEF = 6;

    private static final int[][] indexColumnPositions =
    {
        {SYSROLES_ROLEID, SYSROLES_GRANTEE, SYSROLES_GRANTOR},
        {SYSROLES_ROLEID, SYSROLES_ISDEF},
        {SYSROLES_ROLE_UUID}
    };

    static final int SYSROLES_ROLEID_COLPOS_IN_INDEX_ID_EE_OR = 1;
    static final int SYSROLES_GRANTEE_COLPOS_IN_INDEX_ID_EE_OR = 2;

    // (role)ID_(grant)EE_(grant)OR
    static final int SYSROLES_INDEX_ID_EE_OR_IDX = 0;
    // (role)ID_(is)DEF
    static final int SYSROLES_INDEX_ID_DEF_IDX = 1;
    // UUID
    static final int SYSROLES_INDEX_UUID_IDX = 2;

    private static  final   boolean[]   uniqueness = {
        true,
        false, // many rows have same roleid and is not a definition
        true};

    private static final String[] uuids = {
        "e03f4017-0115-382c-08df-ffffe275b270", // catalog UUID
        "c851401a-0115-382c-08df-ffffe275b270", // heap UUID
        "c065801d-0115-382c-08df-ffffe275b270", // SYSROLES_INDEX_ID_EE_OR
        "787c0020-0115-382c-08df-ffffe275b270", // SYSROLES_INDEX_ID_DEF
        "629f8094-0116-d8f9-5f97-ffffe275b270"  // SYSROLES_INDEX_UUID
    };

    /**
     * Constructor
     *
     * @param uuidf UUIDFactory
     * @param ef    ExecutionFactory
     * @param dvf   DataValueFactory
     */
    public SYSROLESRowFactory(UUIDFactory uuidf,
                       ExecutionFactory ef,
                       DataValueFactory dvf)
    {
        super(uuidf,ef,dvf);
        initInfo(SYSROLES_COLUMN_COUNT, TABLENAME_STRING,
                 indexColumnPositions, uniqueness, uuids );
    }

    /**
     * Make a SYSROLES row
     *
     * @param td a role grant descriptor
     * @param parent unused
     *
     * @return  Row suitable for inserting into SYSROLES.
     *
     * @exception   StandardException thrown on failure
     */

    public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent)
        throws StandardException
    {
        ExecRow                 row;
        String                  oid_string = null;
        String                  roleid = null;
        String                  grantee = null;
        String                  grantor = null;
        boolean                 wao = false;
        boolean                 isdef = false;

        if (td != null)
        {
            RoleGrantDescriptor rgd = (RoleGrantDescriptor)td;

            roleid = rgd.getRoleName();
            grantee = rgd.getGrantee();
            grantor = rgd.getGrantor();
            wao = rgd.isWithAdminOption();
            isdef = rgd.isDef();
            UUID oid = rgd.getUUID();
            oid_string = oid.toString();
        }

        /* Build the row to insert */
        row = getExecutionFactory().getValueRow(SYSROLES_COLUMN_COUNT);

        /* 1st column is UUID */
        row.setColumn(1, new SQLChar(oid_string));

        /* 2nd column is ROLEID */
        row.setColumn(2, new SQLVarchar(roleid));

        /* 3rd column is GRANTEE */
        row.setColumn(3, new SQLVarchar(grantee));

        /* 4th column is GRANTOR */
        row.setColumn(4, new SQLVarchar(grantor));

        /* 5th column is WITHADMINOPTION */
        row.setColumn(5, new SQLChar(wao ? "Y" : "N"));

        /* 6th column is ISDEF */
        row.setColumn(6, new SQLChar(isdef ? "Y" : "N"));

        return row;
    }


    ///////////////////////////////////////////////////////////////////////////
    //
    //  ABSTRACT METHODS TO BE IMPLEMENTED BY CHILDREN OF CatalogRowFactory
    //
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Make an  Tuple Descriptor out of a SYSROLES row
     *
     * @param row                   a SYSROLES row
     * @param parentTupleDescriptor unused
     * @param dd                    dataDictionary
     *
     * @return  a  descriptor equivalent to a SYSROLES row
     *
     * @exception   StandardException thrown on failure
     */
    public TupleDescriptor buildDescriptor
        (ExecRow                 row,
         TupleDescriptor         parentTupleDescriptor,
         DataDictionary          dd )
        throws StandardException {

        DataValueDescriptor         col;
        RoleGrantDescriptor              descriptor;
        String                      oid_string;
        String                      roleid;
        String                      grantee;
        String                      grantor;
        String                      wao;
        String                      isdef;
        DataDescriptorGenerator     ddg = dd.getDataDescriptorGenerator();

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(row.nColumns() == SYSROLES_COLUMN_COUNT,
                                 "Wrong number of columns for a SYSROLES row");
        }

        // first column is uuid of this role grant descriptor (char(36))
        col = row.getColumn(1);
        oid_string = col.getString();

        // second column is roleid (varchar(128))
        col = row.getColumn(2);
        roleid = col.getString();

        // third column is grantee (varchar(128))
        col = row.getColumn(3);
        grantee = col.getString();

        // fourth column is grantor (varchar(128))
        col = row.getColumn(4);
        grantor = col.getString();

        // fifth column is withadminoption (char(1))
        col = row.getColumn(5);
        wao = col.getString();

        // sixth column is isdef (char(1))
        col = row.getColumn(6);
        isdef = col.getString();

        descriptor = ddg.newRoleGrantDescriptor
            (getUUIDFactory().recreateUUID(oid_string),
             roleid,
             grantee,
             grantor,
             wao.equals("Y") ? true: false,
             isdef.equals("Y") ? true: false);

        return descriptor;
    }

    /**
     * Builds a list of columns suitable for creating this Catalog.
     *
     *
     * @return array of SystemColumn suitable for making this catalog.
     */
    public SystemColumn[]   buildColumnList()
        throws StandardException
    {
        return new SystemColumn[] {
            SystemColumnImpl.getUUIDColumn("UUID", false),
            SystemColumnImpl.getIdentifierColumn("ROLEID", false),
            SystemColumnImpl.getIdentifierColumn("GRANTEE", false),
            SystemColumnImpl.getIdentifierColumn("GRANTOR", false),
            SystemColumnImpl.getIndicatorColumn("WITHADMINOPTION"),
            SystemColumnImpl.getIndicatorColumn("ISDEF"),
        };
    }
}
