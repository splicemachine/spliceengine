package com.splicemachine.utils;

/**
 * Our own representation of a MutableBoolean. Allows us to avoid unnecessary imports
 * from external libraries.
 *
 * @author Ao Zeng
 *         Date: 8/3/18
 */
public class ModifiableBoolean {
    private boolean booleanVal;

    /**
     * Constructor
     * @param initVal initial value
     */
    public ModifiableBoolean(boolean initVal){
        this.booleanVal = initVal;
    }

    /**
     * setter
     * @param newVal new boolean value to set
     */
    public void set(boolean newVal){
        this.booleanVal = newVal;
    }

    /**
     * getter
     * @return  the boolean value it contains
     */
    public boolean get(){
        return this.booleanVal;
    }
}
