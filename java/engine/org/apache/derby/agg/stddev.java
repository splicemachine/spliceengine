package org.apache.derby.agg;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.derby.agg.Aggregator;

public class stddev<K extends Double> 
        implements Aggregator<K,K,stddev<K>>
{
    private ArrayList<K> _values;

    public stddev() {}

    public void init() { _values = new ArrayList<K>(); }

    public void accumulate( K value ) { _values.add( value ); }

    public void merge( stddev<K> other )
    { 
        _values.addAll( other._values ); 
    }

    public K terminate()
    {   
        double s = sum();
        
        //Collections.sort( _values );

        int count = _values.size();
        
        double a = s/count;
        double v = var(a);
        double d = Math.sqrt(v);
        Double rt = new Double(d);
        return (K)rt;
    }
    private double sum() {
        double d = 0.0;
        for(int i = 0; i < _values.size(); i++) {
            d = d + _values.get(i).doubleValue();
        }
        return d;
    }
    private double var(double a) {
        double v = 0.0;
        for(int i = 0; i < _values.size(); i++) {
            v = v + (_values.get(i).doubleValue() - a) * (_values.get(i).doubleValue() - a);
        }
        return v;
    }
}

