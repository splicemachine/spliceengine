package org.apache.derby.agg;

import java.util.ArrayList;
import java.util.Collections;
import org.apache.derby.agg.Aggregator;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.error.StandardException;
import java.lang.Math;

public class stddev<K extends Double> 
        implements Aggregator<K,K,stddev<K>>
{
    private ArrayList<K> _values;
    boolean resultComputed;
    Double result;
    
    public stddev() {
    	resultComputed = false;
    }

    public void init() {
    	_values = new ArrayList<K>();
    }

    public void accumulate( K value ) { _values.add( value ); }

    public void merge( stddev<K> other )
    { 
        _values.addAll( other._values ); 
    }

    public K terminate()
    {   
    	if (!resultComputed) {
    	
    		double dev = 0;
    		int count = _values.size();
        	double s = sum();
            
            double a = s / count;
            double v = var(a);
            dev = Math.sqrt(v);
        
            result = new Double(dev);
    	}
        return (K)result;
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
            v = v + Math.pow((_values.get(i).doubleValue() - a), 2);
        }
        return v/_values.size();
    }
    public void add (DataValueDescriptor addend) throws StandardException{
		result = new Double(addend.getDouble());
		resultComputed = true;
	}	

}

