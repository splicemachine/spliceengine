/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.Math;

public class SpliceStddevSamp<K extends Double> extends SpliceUDAVariance<K>
{

    Double result;

    public SpliceStddevSamp() {

    }

    public void init() {
        super.init();
        result = null;
    }

    @Override
    public K terminate() {
       if (result == null) {
           if (count > 1) {
               result = new Double(Math.sqrt(variance/(count-1)));
           } else {
               result = new Double(0);
           }
       }
        return (K) result;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeBoolean(result != null);
        if (result!=null) {
            out.writeDouble(result);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        if (in.readBoolean()) {
            result = in.readDouble();
        }
    }
}

