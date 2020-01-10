/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.storage;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public class MFilterFactory implements DataFilterFactory{
    public static final DataFilterFactory INSTANCE= new MFilterFactory();

    private MFilterFactory(){}

    @Override
    public DataFilter singleColumnEqualsValueFilter(byte[] family,byte[] qualifier,byte[] value){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public DataFilter allocatedFilter(byte[] localAddress){
        //TODO -sf- implement?
        return new DataFilter(){
            @Override
            public ReturnCode filterCell(DataCell keyValue) throws IOException{
                return ReturnCode.INCLUDE;
            }

            @Override
            public boolean filterRow() throws IOException{
                return false;
            }

            @Override
            public void reset() throws IOException{

            }
        };
    }
}
