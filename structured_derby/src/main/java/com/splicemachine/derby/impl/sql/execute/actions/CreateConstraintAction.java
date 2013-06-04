package com.splicemachine.derby.impl.sql.execute.actions;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.depend.ProviderInfo;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.impl.sql.execute.ConstraintInfo;
import org.apache.derby.impl.sql.execute.CreateConstraintConstantAction;
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


