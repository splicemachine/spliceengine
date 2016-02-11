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