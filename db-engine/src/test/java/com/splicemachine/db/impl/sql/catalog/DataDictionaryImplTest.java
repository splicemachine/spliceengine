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

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.NumberDataValue;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.impl.services.uuid.BasicUUID;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Types;
import java.util.Random;

import static com.splicemachine.db.iapi.sql.dictionary.DataDictionary.FIRST_USER_TABLE_NUMBER;

public class DataDictionaryImplTest {
    static class MockDataDictionaryImpl extends DataDictionaryImpl {

        @Override
        protected SystemProcedureGenerator getSystemProcedures() {
            return null;
        }

        @Override
        protected SystemAggregateGenerator getSystemAggregateGenerator() {
            return null;
        }

        @Override
        protected void setDependencyManager() {

        }

        @Override
        protected void addSubKeyConstraint(KeyConstraintDescriptor descriptor, TransactionController tc) throws StandardException {

        }

        @Override
        protected TabInfoImpl getPkTable() throws StandardException {
            return null;
        }

        @Override
        public void getCurrentValueAndAdvance(String sequenceUUIDstring, NumberDataValue returnValue, boolean useBatch) throws StandardException {

        }

        @Override
        public Long peekAtSequence(String schemaName, String sequenceName) throws StandardException {
            return null;
        }

        @Override
        public boolean canWriteCache(TransactionController xactMgr) throws StandardException {
            return false;
        }

        @Override
        public boolean canReadCache(TransactionController xactMgr) throws StandardException {
            return false;
        }

        @Override
        public boolean canUseDependencyManager() {
            return false;
        }

        @Override
        public boolean isMetadataAccessRestrictionEnabled() {
            return false;
        }

        @Override
        public void setMetadataAccessRestrictionEnabled() {

        }

        @Override
        public void updateSystemSchemasView(TransactionController tc) throws StandardException {

        }

        public void testMarkSystemTablesAsVersion1(TableDescriptor td) {
            markSystemTablesAsVersion1(td);
        }
    }

    private Random random = new Random();

    TableDescriptor constructSysTableStatsTableDescriptor(long[] conglomerateIds) {
        assert conglomerateIds.length == 4;
        MockDataDictionaryImpl dataDictionary = new MockDataDictionaryImpl();
        DataDescriptorGenerator ddg = new DataDescriptorGenerator(dataDictionary);
        UUID schemaUuid = new BasicUUID("8000000d-00d0-fd77-3ed8-000a0a0b1900"), tableUuid = new BasicUUID("08264012-014b-c29b-a826-000003009390");
        SchemaDescriptor sd = new SchemaDescriptor(dataDictionary, "SYS", "SPLICE", schemaUuid, true);
        TableDescriptor td = new TableDescriptor(dataDictionary, "SYSTABLESTATS", sd, 1, 'R', -1, null, null, null, null, null, null, false, false,null);
        td.setUUID(new BasicUUID("08264012-014b-c29b-a826-000003009390"));
        td.setVersion("4.0");
        td.getColumnDescriptorList().add(new ColumnDescriptor("CONGLOMERATEID", 1 , 1 , new DataTypeDescriptor(TypeId.BIGINT_ID, true), null, null, tableUuid, null, 0, 0, 0, true,1, (byte) 0));
        td.getColumnDescriptorList().add(new ColumnDescriptor("PARTITIONID"   , 2 , 2 , DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 32672), null, null, tableUuid, null, 0, 0, 0, true,1, (byte) 0));
        td.getColumnDescriptorList().add(new ColumnDescriptor("LAST_UPDATED"  , 3 , 3 , DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.TIMESTAMP), null, null, tableUuid, null, 0, 0, 0, true,1, (byte) 0));
        td.getColumnDescriptorList().add(new ColumnDescriptor("IS_STALE"      , 4 , 4 , DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN), null, null, tableUuid, null, 0, 0, 0, true,1, (byte) 0));
        td.getColumnDescriptorList().add(new ColumnDescriptor("IN_PROGRESS"   , 5 , 5 , DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN), null, null, tableUuid, null, 0, 0, 0, true,1, (byte) 0));
        td.getColumnDescriptorList().add(new ColumnDescriptor("ROWCOUNT"      , 6 , 6 , DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT), null, null, tableUuid, null, 0, 0, 0, true,1, (byte) 0));
        td.getColumnDescriptorList().add(new ColumnDescriptor("PARTITION_SIZE", 7 , 7 , DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT), null, null, tableUuid, null, 0, 0, 0, true,1, (byte) 0));
        td.getColumnDescriptorList().add(new ColumnDescriptor("MEANROWWIDTH"  , 8 , 8 , DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER), null, null, tableUuid, null, 0, 0, 0, true,1, (byte) 0));
        td.getColumnDescriptorList().add(new ColumnDescriptor("NUMPARTITIONS" , 9 , 9 , DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT), null, null, tableUuid, null, 0, 0, 0, true,1, (byte) 0));
        td.getColumnDescriptorList().add(new ColumnDescriptor("STATSUTYPE"    , 10, 10, DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER), null, null, tableUuid, null, 0, 0, 0, true,1, (byte) 0));
        td.getColumnDescriptorList().add(new ColumnDescriptor("SAMPLEFRACTION", 11, 11, DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.DOUBLE), null, null, tableUuid, null, 0, 0, 0, true,1, (byte) 0));
        td.getConglomerateDescriptorList().add(ddg.newConglomerateDescriptor(conglomerateIds[0], "SYSTABLESTATS_INDEX1", true, new IndexRowGenerator(), false, tableUuid, tableUuid, schemaUuid));
        td.getConglomerateDescriptorList().add(ddg.newConglomerateDescriptor(conglomerateIds[1], "SYSTABLESTATS_INDEX2", true, new IndexRowGenerator(), false, tableUuid, tableUuid, schemaUuid));
        td.getConglomerateDescriptorList().add(ddg.newConglomerateDescriptor(conglomerateIds[2], "SYSTABLESTATS_INDEX3", true, new IndexRowGenerator(), false, tableUuid, tableUuid, schemaUuid));
        td.getConglomerateDescriptorList().add(ddg.newConglomerateDescriptor(conglomerateIds[3], "SYSTABLESTATS_HEAP", false, new IndexRowGenerator(), false, tableUuid, tableUuid, schemaUuid));
        return td;
    }

    TableDescriptor constructSysTableStatsTableDescriptorBeforeUpgrade() {
        return constructSysTableStatsTableDescriptor(new long[] {random.nextInt((int)FIRST_USER_TABLE_NUMBER), random.nextInt((int)FIRST_USER_TABLE_NUMBER), random.nextInt((int)FIRST_USER_TABLE_NUMBER), random.nextInt((int)FIRST_USER_TABLE_NUMBER)});
    }

    TableDescriptor constructSysTableStatsTableDescriptorAfterUpgrade() {
        final int UPPER_BOUND = 1000;
        return constructSysTableStatsTableDescriptor(new long[] {random.nextInt(UPPER_BOUND) + (int)FIRST_USER_TABLE_NUMBER, random.nextInt(UPPER_BOUND) + (int)FIRST_USER_TABLE_NUMBER, random.nextInt(UPPER_BOUND) + (int)FIRST_USER_TABLE_NUMBER, random.nextInt(UPPER_BOUND) + (int)FIRST_USER_TABLE_NUMBER});
    }

    @Test
    public void systemTableVersionIsSetCorrectlyBeforeUpgrade() {
        MockDataDictionaryImpl dataDictionary = new MockDataDictionaryImpl();
        TableDescriptor td = constructSysTableStatsTableDescriptorBeforeUpgrade();
        dataDictionary.testMarkSystemTablesAsVersion1(td);
        Assert.assertEquals("1.0", td.getVersion());
    }

    @Test
    public void systemTableVersionIsSetCorrectlyAfterUpgrade() {
        MockDataDictionaryImpl dataDictionary = new MockDataDictionaryImpl();
        TableDescriptor td = constructSysTableStatsTableDescriptorAfterUpgrade();
        dataDictionary.testMarkSystemTablesAsVersion1(td);
        Assert.assertEquals("1.0", td.getVersion());
    }
}
