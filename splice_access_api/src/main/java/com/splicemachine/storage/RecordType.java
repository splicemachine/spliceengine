package com.splicemachine.storage;

/**
 *
 * Record Type
 *
 */
public enum RecordType {
    INSERT((byte)0x01),
    UPDATE((byte)0x02),
    DELETE((byte)0x03),
    UPSERT((byte)0x05),
    CANCEL((byte)0x08); /* For import process to cancel out an inserted row that violates a unique constraint */

    private final byte typeCode;

    private RecordType(byte typeCode) { this.typeCode = typeCode; }

    public static RecordType decode(byte typeByte) {
        for(RecordType type:values()){
            if(type.typeCode==typeByte) return type;
        }
        throw new IllegalArgumentException("Incorrect typeByte "+ typeByte);
    }

    public byte asByte() {
        return typeCode;
    }

    public boolean isUpdateOrUpsert() {
        return UPDATE.equals(this) || UPSERT.equals(this);
    }
}
