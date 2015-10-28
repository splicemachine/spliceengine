package com.splicemachine.derby.impl.sql.execute.dvd;

import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.types.*;

/**
 * @author Scott Fines
 *         Date: 10/28/15
 */
public class LazyDouble extends LazyNumberDataValueDescriptor{

    public LazyDouble(){
    }

    public LazyDouble(NumberDataValue ndv){
        super(ndv);
    }

    @Override
    public int getTypeFormatId(){
        return StoredFormatIds.SQL_DOUBLE_ID;
    }

    @Override
    public int typePrecedence(){
        return TypeId.DOUBLE_PRECEDENCE;
    }

    @Override
    protected DataValueDescriptor newDescriptor(){
        ndv = new SQLDouble();
        return ndv;
    }

    @Override
    public DataValueDescriptor cloneHolder(){
        if(isNull()) return new LazyDouble();
        else if(isDeserialized())
            return new LazyDouble(ndv);
        else{
            //return a shallow clone, pointing to the same bytes
            LazyDouble ld = new LazyDouble();
            ld.initForDeserialization(tableVersion,serializer,bytes,offset,length,descendingOrder);
            return ld;
        }
    }

    @Override
    public DataValueDescriptor cloneValue(boolean forceMaterialization){
        if(this.isSerialized()){
            LazyNumberDataValueDescriptor lsdv=new LazyDouble();
            lsdv.initForDeserialization(tableVersion,serializer,bytes,offset,length,descendingOrder);
            return lsdv;
        }
        forceDeserialization();
        return new LazyDouble((NumberDataValue)ndv.cloneValue(forceMaterialization));
    }

    @Override
    public DataValueDescriptor getNewNull(){
        return new LazyDouble();
    }

    @Override
    public boolean isDoubleType(){
        return true;
    }
}
