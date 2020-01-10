/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.dbTesting.functionTests.tests.lang;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.math.BigDecimal;
import java.sql.Timestamp;

/**
 * Sample UDT for tests.
 */
public class Price implements Externalizable
{
    // initial version id
    private static final int FIRST_VERSION = 0;
    private static final int TIMESTAMPED_VERSION = FIRST_VERSION + 1;

    private static final Timestamp DEFAULT_TIMESTAMP = new Timestamp( 0L );

    private static Price _savedPrice;

    public String currencyCode;
    public BigDecimal amount;
    public Timestamp timeInstant;

    // methods to be registered as functions
    public static Price makePrice( ) { return new Price( "USD", BigDecimal.valueOf(1L), DEFAULT_TIMESTAMP ); }
    public static Price makePrice( String currencyCode, BigDecimal amount, Timestamp timeInstant ) { return new Price( currencyCode, amount, timeInstant ); }
    public static String getCurrencyCode( Price price ) { return price.currencyCode; }
    public static BigDecimal getAmount( Price price ) { return price.amount; }
    public static Timestamp getTimeInstant( Price price ) { return price.timeInstant; }
    public static void savePrice( Price price ) { _savedPrice = price; }
    public static Price getSavedPrice() { return _savedPrice; }

    // 0-arg constructor needed by Externalizable machinery
    public Price() {}

    public Price( String currencyCode, BigDecimal amount, Timestamp timeInstant )
    {
        this.currencyCode = currencyCode;
        this.amount = amount;
        this.timeInstant = timeInstant;
    }

    public String toString()
    {
        StringBuffer buffer = new StringBuffer();

        buffer.append( "Price( " + currencyCode + ", " + amount + ", "  );
        if ( DEFAULT_TIMESTAMP.equals( timeInstant ) ) { buffer.append( "XXX" ); }
        else { buffer.append( timeInstant ); }
        buffer.append( " )" );

        return buffer.toString();
    }

    public boolean equals( Object other )
    {
        if ( other == null )  { return false; }
        if ( !(other instanceof Price) ) { return false; }

        Price that = (Price) other;

        return this.toString().equals( that.toString() );
    }

    // Externalizable implementation
    public void writeExternal(ObjectOutput out) throws IOException
    {
        // first write the version id
        out.writeInt( TIMESTAMPED_VERSION );

        // now write the state
        out.writeObject( currencyCode );
        out.writeObject( amount );
        out.writeObject( timeInstant );
    }  
    public void readExternal(ObjectInput in)throws IOException, ClassNotFoundException
    {
        // read the version id
        int oldVersion = in.readInt();
        if ( oldVersion < FIRST_VERSION ) { throw new IOException( "Corrupt data stream." ); }
        if ( oldVersion > TIMESTAMPED_VERSION ) { throw new IOException( "Can't deserialize from the future." ); }

        currencyCode = (String) in.readObject();
        amount = (BigDecimal) in.readObject();

        if ( oldVersion >= TIMESTAMPED_VERSION ) { timeInstant = (Timestamp) in.readObject(); }
        else { timeInstant = DEFAULT_TIMESTAMP; }
    }
}
