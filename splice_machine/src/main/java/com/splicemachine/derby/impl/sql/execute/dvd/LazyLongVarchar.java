package com.splicemachine.derby.impl.sql.execute.dvd;

import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLLongvarchar;
import com.splicemachine.db.iapi.types.StringDataValue;
import com.splicemachine.db.iapi.types.TypeId;

/**
 * @author Scott Fines
 *         Date: 10/27/15
 */
public class LazyLongVarchar extends LazyStringDataValueDescriptor{

    public LazyLongVarchar(){
    }

    public LazyLongVarchar(StringDataValue sdv){
        super(sdv);
    }

    @Override
    public DataValueDescriptor cloneHolder(){
        forceDeserialization();
        return new LazyLongVarchar(sdv);
    }

    @Override
    public DataValueDescriptor cloneValue(boolean forceMaterialization){
        if(this.isSerialized()){
            LazyStringDataValueDescriptor lsdv=new LazyLongVarchar();
            lsdv.initForDeserialization(tableVersion,serializer,bytes,offset,length,descendingOrder);
            return lsdv;
        }
        forceDeserialization();
        return new LazyLongVarchar((StringDataValue)sdv.cloneValue(forceMaterialization));
    }

    @Override
    public DataValueDescriptor getNewNull(){
        return new LazyLongVarchar();
    }

    @Override
    protected DataValueDescriptor newDescriptor(){
        sdv = new SQLLongvarchar();
        return sdv;
    }


    @Override
    public int getTypeFormatId(){
        return StoredFormatIds.SQL_LONGVARCHAR_ID;
    }

    @Override
    public int typePrecedence(){
        return TypeId.LONGVARCHAR_PRECEDENCE;
    }
}
