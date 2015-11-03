package com.splicemachine.derby.impl.sql.execute.dvd;

import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.iapi.types.StringDataValue;
import com.splicemachine.db.iapi.types.TypeId;

/**
 * @author Scott Fines
 *         Date: 10/27/15
 */
public class LazyVarchar extends LazyStringDataValueDescriptor{

    public LazyVarchar(){
    }

    public LazyVarchar(StringDataValue sdv){
        super(sdv);
    }

    @Override
    public int getTypeFormatId(){
        return StoredFormatIds.SQL_VARCHAR_ID;
    }

    @Override
    public int typePrecedence(){
        return TypeId.VARCHAR_PRECEDENCE;
    }

    @Override
    protected DataValueDescriptor newDescriptor(){
        sdv = new SQLVarchar();
        return sdv;
    }

    @Override
    public DataValueDescriptor cloneHolder(){
        if(isNull())
            return new LazyVarchar();
        else if(isDeserialized())
            return new LazyVarchar(sdv);
        else{
            /*
             * Return a shallow clone, so just point to the same bytes
             */
            LazyVarchar lv = new LazyVarchar();
            lv.initForDeserialization(tableVersion,serializer,bytes,offset,length,descendingOrder);
            return lv;
        }
    }

    @Override
    public DataValueDescriptor cloneValue(boolean forceMaterialization){
        if(isNull())
            return new LazyVarchar();
        else if(this.isSerialized()){
            LazyStringDataValueDescriptor lsdv=new LazyVarchar();
            lsdv.initForDeserialization(tableVersion,serializer,bytes,offset,length,descendingOrder);
            return lsdv;
        }else{
            if(sdv==null)
                return new LazyVarchar((StringDataValue)newDescriptor());
            else{
                return new LazyVarchar((StringDataValue)sdv.cloneValue(forceMaterialization));
            }
        }
    }

    @Override
    public DataValueDescriptor getNewNull(){
        return new LazyVarchar();
    }
}
