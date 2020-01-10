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

import com.splicemachine.db.shared.common.udt.UDTBase;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class Price extends UDTBase
{
  private static final int FIRST_VERSION = 0;
  public String currencyCode;
  public double amount;
  private static final long serialVersionUID = -3305252721311660886L;

  public static double getAmount(Price price ) { return price.amount; }

  public Price()
  {
  }

  public Price(String paramString, double paramDouble)
  {
    this.currencyCode = paramString;
    this.amount = paramDouble;
  }

  public void writeExternal(ObjectOutput paramObjectOutput)
    throws IOException
  {
    super.writeExternal(paramObjectOutput);

    paramObjectOutput.writeBoolean(this.currencyCode != null);
    if (this.currencyCode != null) {
      paramObjectOutput.writeUTF(this.currencyCode);
    }
    paramObjectOutput.writeDouble(this.amount);
  }

  public void readExternal(ObjectInput paramObjectInput)
    throws IOException, ClassNotFoundException
  {
    super.readExternal(paramObjectInput);

    if (paramObjectInput.readBoolean()) {
      this.currencyCode = paramObjectInput.readUTF();
    }
    this.amount = paramObjectInput.readDouble();
  }
}
