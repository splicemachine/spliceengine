package com.splicemachine.serialization;

/** The sort order of a row key, ascending or descending. */
public enum Order
{ 
  ASCENDING((byte)0), 
  DESCENDING((byte)0xff);

  private final byte mask;

  Order(byte mask) { this.mask = mask; }

  /** Gets the byte mask associated with the sort order. When a 
   * serialized byte is XOR'd with the mask, the result is the same byte 
   * but sorted in the direction specified by the Order object. 
   * @see RowKey#serialize
   */
  byte mask() { return mask; }

}
