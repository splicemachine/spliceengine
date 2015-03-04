package com.splicemachine.derby.impl.sql.catalog;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.DataValueFactory;
import com.splicemachine.db.iapi.types.SQLChar;
import com.splicemachine.db.impl.services.uuid.BasicUUID;
import com.splicemachine.db.impl.sql.catalog.SystemColumnImpl;

/**
 * @author Scott Fines
 * Created on: 3/1/13
 */
public class SYSPRIMARYKEYSRowFactory extends CatalogRowFactory {

    private static final String TABLENAME_STRING = "SYSPRIMARYKEYS";

    protected static final int SYSPRIMARYKEYS_COLUMN_COUNT=2;

    /*Column position numbers */
    protected static final int SYSPRIMARYKEYS_CONSTRAINTID=1;
    protected static final int SYSPRIMARYKEYS_CONGLOMERATEID=2;

    private static final boolean[] uniqueness = null;

    private static final String[] uuids = new String[]{
            "f48ad515-013d-35d6-f400-6915f6177d2f",
            "f48ad516-013d-35d6-f400-6915f6177d2f"
    };

    public SYSPRIMARYKEYSRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf) {
        super(uuidf, ef, dvf);
        initInfo(SYSPRIMARYKEYS_COLUMN_COUNT,TABLENAME_STRING,null,uniqueness,uuids);
    }

    @Override
    public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent) throws StandardException {
        UUID oid;
        String constraintOid = null;
        String conglomerateId = null;
        if(td!=null){
            KeyConstraintDescriptor constraint = (KeyConstraintDescriptor)td;

            oid = constraint.getUUID();
            constraintOid = oid.toString();

            //find the Table conglomerate UUID
            ConglomerateDescriptorList cdl = constraint.getTableDescriptor().getConglomerateDescriptorList();
            for(int index=0;index<cdl.size();index++){
                ConglomerateDescriptor cd = (ConglomerateDescriptor) cdl.get(index);
                TableDescriptor tableDescriptor = constraint.getTableDescriptor();
                if(tableDescriptor.getHeapConglomerateId()==cd.getConglomerateNumber()){
                    conglomerateId = cd.getUUID().toString();
                    break;
                }
            }
        }

        ExecRow row = getExecutionFactory().getValueRow(SYSPRIMARYKEYS_COLUMN_COUNT);
        row.setColumn(SYSPRIMARYKEYS_CONSTRAINTID,new SQLChar(constraintOid));
        row.setColumn(SYSPRIMARYKEYS_CONGLOMERATEID,new SQLChar(conglomerateId));

        return row;
    }

    @Override
    public TupleDescriptor buildDescriptor(ExecRow row,
                                           TupleDescriptor parentTuple,
                                           DataDictionary dataDictionary)
            throws StandardException {
        if(SanityManager.DEBUG){
            SanityManager.ASSERT(
                    row.nColumns()==SYSPRIMARYKEYS_COLUMN_COUNT,
                    "Wrong number of columns for a SYSPRIMARYKEYS row");
        }

        DataDescriptorGenerator ddg = dataDictionary.getDataDescriptorGenerator();

        /*
         * First column is a constraint UUID
         * Second column is the conglomerate for the table with the PK constraint
         */
        DataValueDescriptor col = row.getColumn(SYSPRIMARYKEYS_CONSTRAINTID);
        String constraintUUIDString = col.getString();
        UUID constraintUUID = getUUIDFactory().recreateUUID(constraintUUIDString);

        col = row.getColumn(SYSPRIMARYKEYS_CONGLOMERATEID);
        String conglomerateUUIDString = col.getString();
        UUID conglomerateUUID = getUUIDFactory().recreateUUID(conglomerateUUIDString);

        return new SubKeyConstraintDescriptor(constraintUUID,conglomerateUUID);
    }

    @Override
    public SystemColumn[] buildColumnList() throws StandardException {
        return new SystemColumn[]{
                SystemColumnImpl.getUUIDColumn("CONSTRAINTID", false),
                SystemColumnImpl.getUUIDColumn("CONGLOMERATEID",false)
        };
    }

    public static void main(String... args) throws Exception{

        BasicUUID one = new BasicUUID(50369424,1362416594897l,136724498);

        BasicUUID two = new BasicUUID(50369424,1362416594897l,-1607811053);

        System.out.printf("one=%s,two=%s,one.toString().equals(two.toString())=%s%n",
                one,two,one.toString().equals(two.toString()));

    }
}
