/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

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
        return new LazyLongVarchar(dvd);
    }

    @Override
    public DataValueDescriptor cloneValue(boolean forceMaterialization){
        if(this.isSerialized()){
            LazyStringDataValueDescriptor lsdv=new LazyLongVarchar();
            lsdv.initForDeserialization(tableVersion,serializer,bytes,offset,length,descendingOrder);
            return lsdv;
        }
        forceDeserialization();
        return new LazyLongVarchar((StringDataValue)dvd.cloneValue(forceMaterialization));
    }

    @Override
    public DataValueDescriptor getNewNull(){
        return new LazyLongVarchar();
    }

    @Override
    protected StringDataValue newDescriptor(){
        return new SQLLongvarchar();
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
