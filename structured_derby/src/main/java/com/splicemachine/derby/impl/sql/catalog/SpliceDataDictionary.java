package com.splicemachine.derby.impl.sql.catalog;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.impl.sql.catalog.DataDictionaryImpl;
import org.apache.derby.impl.sql.catalog.SystemProcedureGenerator;

/**
 * @author Scott Fines
 *         Created on: 2/28/13
 */
public class SpliceDataDictionary extends DataDictionaryImpl {

    @Override
    protected SystemProcedureGenerator getSystemProcedures() {
        return new SpliceSystemProcedures(this);
    }

    public static void main(String... args) throws Exception{
        System.out.println(DataDictionary.class.isAssignableFrom(SpliceDataDictionary.class));
        System.out.println(SpliceDataDictionary.class.isAssignableFrom(DataDictionary.class));
    }
}
