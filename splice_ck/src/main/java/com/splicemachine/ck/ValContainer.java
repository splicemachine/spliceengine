package com.splicemachine.ck;

public class ValContainer<T> {
    private T val;

    public ValContainer() {
    }

    public ValContainer(T v) {
        this.val = v;
    }

    public T get() {
        return val;
    }

    public void set(T val) {
        this.val = val;
    }
}