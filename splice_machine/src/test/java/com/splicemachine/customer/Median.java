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

package com.splicemachine.customer;

import com.splicemachine.db.agg.Aggregator;

import java.util.ArrayList;
import java.util.Collections;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class Median<V extends Comparable<V>>
  implements Aggregator<V, V, Median<V>>, Externalizable
{
  private ArrayList<V> _values;

    public Median() {}

    public void init() { _values = new ArrayList<V>(); }

    public void accumulate( V value ) { _values.add( value ); }

    public void merge( Median<V> other )
    { 
        _values.addAll( other._values ); 
    }

    public V terminate()
    {
        Collections.sort( _values );

        int count = _values.size();
        
        if ( count == 0 ) { return null; }
        else { return _values.get( count/2 ); }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(_values.size());
        for (int i = 0; i < _values.size(); ++i) {
            out.writeObject(_values.get(i));
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int n = in.readInt();
        _values = new ArrayList();
        for (int i = 0; i < n; ++i) {
            _values.add(i, (V)in.readObject());
        }
    }

}
