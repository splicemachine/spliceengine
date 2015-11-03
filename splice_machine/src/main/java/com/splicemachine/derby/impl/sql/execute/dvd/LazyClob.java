package com.splicemachine.derby.impl.sql.execute.dvd;

import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLClob;
import com.splicemachine.db.iapi.types.StringDataValue;
import com.splicemachine.db.iapi.types.TypeId;

/**
 * @author Scott Fines
 *         Date: 10/27/15
 */
public class LazyClob extends LazyStringDataValueDescriptor{
    public LazyClob(){
    }

    public LazyClob(StringDataValue sdv){
        super(sdv);
    }

    @Override
    protected DataValueDescriptor newDescriptor(){
        sdv = new SQLClob();
        return sdv;
    }

    @Override
    public DataValueDescriptor cloneHolder(){
        forceDeserialization();
        return new LazyClob(sdv);
    }

    @Override
    public DataValueDescriptor cloneValue(boolean forceMaterialization){
        if(this.isSerialized()){
            LazyStringDataValueDescriptor lsdv=new LazyClob();
            lsdv.initForDeserialization(tableVersion,serializer,bytes,offset,length,descendingOrder);
            return lsdv;
        }
        forceDeserialization();
        return new LazyClob((StringDataValue)sdv.cloneValue(forceMaterialization));
    }

    @Override
    public DataValueDescriptor getNewNull(){
        return new LazyClob();
    }

    @Override
    public int getTypeFormatId(){
        return StoredFormatIds.SQL_CLOB_ID;
    }

    @Override
    public int typePrecedence(){
        return TypeId.CLOB_PRECEDENCE;
    }
}
