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