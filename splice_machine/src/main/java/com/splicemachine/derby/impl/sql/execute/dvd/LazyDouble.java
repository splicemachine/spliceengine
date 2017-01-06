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
    protected NumberDataValue newDescriptor(){
        return new SQLDouble();
    }

    @Override
    public DataValueDescriptor cloneHolder(){
        if(isNull()) return new LazyDouble();
        else if(isDeserialized())
            return new LazyDouble(dvd);
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
        return new LazyDouble((NumberDataValue)dvd.cloneValue(forceMaterialization));
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
