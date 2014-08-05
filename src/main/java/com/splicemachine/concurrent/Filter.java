package com.splicemachine.concurrent;

import java.nio.ByteBuffer;

public abstract class Filter{
    int hashCount;
    int getHashCount(){
        return hashCount;
    }
    public abstract void add(ByteBuffer key);
    public abstract boolean isPresent(ByteBuffer key);
}
