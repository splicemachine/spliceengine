package com.splicemachine.mrio.api.core;

/**
 * Created by jyuan on 5/8/15.
 */
public class PKColumnNamePosition {
    private String name;
    private int position;

    public PKColumnNamePosition(){}

    public PKColumnNamePosition(String name, int position) {
        this.name = name;
        this.position = position;
    }

    public String getName() {
        return name;
    }

    public int getPosition() {
        return position;
    }

    public String toString() {
        return "(" + name + "," + position + ")";
    }
}
