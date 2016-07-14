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