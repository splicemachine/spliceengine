package com.splicemachine.derby.impl.sql.execute.dvd;

import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLChar;
import com.splicemachine.db.iapi.types.StringDataValue;
import com.splicemachine.db.iapi.types.TypeId;

/**
 * @author Scott Fines
 *         Date: 10/27/15
 */
public class LazyChar extends LazyStringDataValueDescriptor{

    public LazyChar(){ }

    public LazyChar(StringDataValue sdv){
        super(sdv);
    }

    @Override
    public DataValueDescriptor cloneHolder(){
        forceDeserialization();
        return new LazyChar(sdv);
    }

    @Override
    public DataValueDescriptor cloneValue(boolean forceMaterialization){
        if(this.isSerialized()){
            LazyStringDataValueDescriptor lsdv=new LazyChar();
            lsdv.initForDeserialization(tableVersion,serializer,bytes,offset,length,descendingOrder);
            return lsdv;
        }
        forceDeserialization();
        return new LazyChar((StringDataValue)sdv.cloneValue(forceMaterialization));
    }

    @Override
    public DataValueDescriptor getNewNull(){
        return new LazyChar();
    }

    @Override
    protected DataValueDescriptor newDescriptor(){
        sdv = new SQLChar();
        return sdv;
    }

    @Override
    public int getTypeFormatId(){
        return StoredFormatIds.SQL_CHAR_ID;
    }

    @Override
    public int typePrecedence(){
        return TypeId.CHAR_PRECEDENCE;
    }
}
