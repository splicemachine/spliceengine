package com.splicemachine.customer;

public class CreatePrice
{
  public static Price createPriceObject(String paramString, double paramDouble)
  {
    return new Price(paramString, paramDouble);
  }
}