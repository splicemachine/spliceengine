package com.splicemachine.storage;

/**
 * Type of Predicate used. Used for efficient serialization.
 *
 * @author Scott Fines
 * Created on: 8/12/13
 */
public enum PredicateType {
    VALUE((byte)0x01),
    NULL((byte)0x02),
    AND((byte)0x03),
    OR((byte)0x04),
    CUSTOM((byte)0x05),
		CHAR_VALUE((byte)0x06);

    private final byte type;

    private PredicateType(byte type) {
        this.type = type;
    }

    public byte byteValue(){
        return type;
    }

    public static PredicateType valueOf(byte typeByte) {
				if(CHAR_VALUE.type==typeByte)
						return CHAR_VALUE;
        if(VALUE.type==typeByte)
            return VALUE;
        else if(NULL.type==typeByte)
            return NULL;
        else if(AND.type==typeByte)
            return AND;
        else if(OR.type==typeByte)
            return OR;
        else
            return CUSTOM;
    }
}
