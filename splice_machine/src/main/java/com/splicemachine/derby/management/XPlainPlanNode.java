package com.splicemachine.derby.management;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jyuan on 6/10/14.
 */
public class XPlainPlanNode {

    private int level;
    private String name;
    private double rowCount;
    private double cost;
    List<XPlainPlanNode> children;

    public XPlainPlanNode(String name, double rowCount, double cost, int level) {
        this.name = name;
        this.rowCount = rowCount;
        this.cost = cost;
        this.level = level;
        children = new ArrayList<>(2);
    }

    public double getRowCount() {
        return rowCount;
    }

    public double getCost() {
        return cost;
    }

    public void setCost(double cost) {
        this.cost = cost;
    }

    public void setRowCount(double rowCount) {
        this.rowCount = rowCount;
    }

    public int getLevel() {
        return level;
    }

    public void addChild(XPlainPlanNode node) {
        children.add(node);
    }

    public List<XPlainPlanNode> getChildren() {
        return children;
    }

    public String toString() {
        StringBuilder s = new StringBuilder();
        for (int i = 0; i < level; ++i) {
            s.append("   ");
        }

        s.append(name).append("(cost=").append(cost).append(", ").
                append("rowCount=").append(rowCount).append(")");
        return s.toString();
    }
}
