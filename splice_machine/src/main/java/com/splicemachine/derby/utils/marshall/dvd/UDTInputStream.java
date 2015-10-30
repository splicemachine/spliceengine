package com.splicemachine.derby.utils.marshall.dvd;

import com.splicemachine.db.iapi.services.loader.ClassFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

/**
 * Created by jyuan on 11/3/15.
 */
public class UDTInputStream extends ObjectInputStream {

    private ClassFactory cf;

    protected UDTInputStream() throws IOException, SecurityException {
        super();
    }

    public UDTInputStream(InputStream in, ClassFactory cf) throws IOException{
        super(in);
        this.cf = cf;
    }

    @Override
    protected Class<?> resolveClass(ObjectStreamClass desc)
            throws IOException, ClassNotFoundException
    {
        String name = desc.getName();
        return cf.loadApplicationClass(name);
    }
}
