package com.splicemachine.derby.impl.sql.execute.dvd;

import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.NumberDataValue;
import com.splicemachine.db.iapi.types.SQLDecimal;
import com.splicemachine.db.iapi.types.TypeId;

/**
 * @author Scott Fines
 *         Date: 10/28/15
 */
public class LazyDecimal extends LazyNumberDataValueDescriptor{

    public LazyDecimal(){
    }

    public LazyDecimal(NumberDataValue ndv){
        super(ndv);
    }

    @Override
    public int getTypeFormatId(){
        return StoredFormatIds.SQL_DECIMAL_ID;
    }

    @Override
    public int typePrecedence(){
        return TypeId.DECIMAL_PRECEDENCE;
    }

    @Override
    protected DataValueDescriptor newDescriptor(){
        ndv = new SQLDecimal();
        return ndv;
    }

    @Override
    public DataValueDescriptor cloneHolder(){
        forceDeserialization();
        return new LazyDecimal(ndv);
    }

    @Override
    public DataValueDescriptor cloneValue(boolean forceMaterialization){
        if(this.isSerialized()){
            LazyNumberDataValueDescriptor lsdv=new LazyDecimal();
            lsdv.initForDeserialization(tableVersion,serializer,bytes,offset,length,descendingOrder);
            return lsdv;
        }
        forceDeserialization();
        return new LazyDecimal((NumberDataValue)ndv.cloneValue(forceMaterialization));
    }

    @Override
    public DataValueDescriptor getNewNull(){
        return new LazyDecimal();
    }

    @Override
    public boolean isDoubleType(){
        return false;
    }
}
