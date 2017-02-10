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

package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.depend.ProviderInfo;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.impl.sql.execute.ConstraintInfo;
import com.splicemachine.db.impl.sql.execute.CreateConstraintConstantAction;
import org.apache.log4j.Logger;

import com.splicemachine.utils.SpliceLogUtils;

/**
 * @author Scott Fines
 *         Created on: 4/29/13
 */
public class CreateConstraintAction extends CreateConstraintConstantAction {
	private static final Logger LOG = Logger.getLogger(CreateConstraintAction.class);
    /**
     * Make one of these puppies.
     *
     * @param constraintName  Constraint name.
     * @param constraintType  Constraint type.
     * @param forCreateTable  Constraint is being added for a CREATE TABLE
     * @param tableName       Table name.
     * @param tableId         UUID of table.
     * @param schemaName      the schema that table and constraint lives in.
     * @param columnNames     String[] for column names
     * @param indexAction     IndexConstantAction for constraint (if necessary)
     * @param constraintText  Text for check constraint
     *                        RESOLVE - the next parameter should go away once we use UUIDs
     *                        (Generated constraint names will be based off of uuids)
     * @param enabled         Should the constraint be created as enabled
     *                        (enabled == true), or disabled (enabled == false).
     * @param otherConstraint information about the constraint that this references
     * @param providerInfo    Information on all the Providers
     */
    public CreateConstraintAction(String constraintName, int constraintType, boolean forCreateTable, String tableName, UUID tableId, String schemaName, String[] columnNames, ConstantAction indexAction, String constraintText, boolean enabled, ConstraintInfo otherConstraint, ProviderInfo[] providerInfo) {
        super(constraintName, constraintType, forCreateTable, tableName, tableId, schemaName, columnNames, indexAction, constraintText, enabled, otherConstraint, providerInfo);
    	SpliceLogUtils.trace(LOG, "CreateConstraintAction with name %s for table %s",constraintName,tableName);
    }

    @Override
    protected UUID manageIndexAction(TableDescriptor td, UUIDFactory uuidFactory, Activation activation) throws StandardException {
    	SpliceLogUtils.trace(LOG, "manageIndexAction for table %s with activation %s",td,activation);
        if(indexAction instanceof CreateIndexConstantOperation){
            String backingIndexName;
            CreateIndexConstantOperation cio = (CreateIndexConstantOperation)indexAction;
            if(cio.getIndexName()==null){
                backingIndexName = uuidFactory.createUUID().toString();
                cio.setIndexName(backingIndexName);
            } else {
                backingIndexName = cio.getIndexName();
            }

            indexAction.executeConstantAction(activation);

            ConglomerateDescriptor[] conglomDescs = td.getConglomerateDescriptors();

            ConglomerateDescriptor indexConglom = null;
            for(ConglomerateDescriptor conglomDesc:conglomDescs){
                if(conglomDesc.isIndex() && backingIndexName.equals(conglomDesc.getConglomerateName())){
                    indexConglom = conglomDesc;
                    break;
                }
            }

            if (SanityManager.DEBUG) {
                SanityManager.ASSERT(indexConglom != null,"indexConglom is expected to be non-null after search for backing index");
                SanityManager.ASSERT(indexConglom.isIndex(),"indexConglom is expected to be indexable after search for backing index");
                SanityManager.ASSERT(indexConglom.getConglomerateName().equals(backingIndexName),"indexConglom name expected to be the same as backing index name after search for backing index");
            }

            return indexConglom.getUUID();
        } else {
            return super.manageIndexAction(td, uuidFactory, activation);
        }
    }
}


