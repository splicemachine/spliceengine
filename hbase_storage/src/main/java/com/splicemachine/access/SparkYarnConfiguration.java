/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.access;

import java.io.*;

/**
 * A serializable class for storing YARN and Spark properties in Zookeeper.
 *
 */
public class SparkYarnConfiguration implements Externalizable {

    public SparkYarnConfiguration() {}

    private int numNodes;

    // Yarn properties
    private String yarnNodemanagerResourceMemoryMB;

    // Spark Properties
    private String dynamicAllocationEnabled;
    private String executorInstances;
    private String executorCores;
    private String executorMemory;
    private String dynamicAllocationMaxExecutors;
    private String executorMemoryOverhead;
    private String yarnExecutorMemoryOverhead;

    public boolean equals(SparkYarnConfiguration other) {
        if (numNodes != other.getNumNodes())
            return false;
        if (!stringEquals(yarnNodemanagerResourceMemoryMB, other.getYarnNodemanagerResourceMemoryMB()))
            return false;
        if (!stringEquals(dynamicAllocationEnabled, other.getDynamicAllocationEnabled()))
            return false;
        if (!stringEquals(executorInstances, other.getExecutorInstances()))
            return false;
        if (!stringEquals(executorCores, other.getExecutorCores()))
            return false;
        if (!stringEquals(executorMemory, other.getExecutorMemory()))
            return false;
        if (!stringEquals(dynamicAllocationMaxExecutors, other.getDynamicAllocationMaxExecutors()))
            return false;
        if (!stringEquals(executorMemoryOverhead, other.getExecutorMemoryOverhead()))
            return false;
        if (!stringEquals(yarnExecutorMemoryOverhead, other.getYarnExecutorMemoryOverhead()))
            return false;

        return true;
    }

    public int hashCode() {
        int hc = numNodes;
        hc = hc*31 + (yarnNodemanagerResourceMemoryMB == null ? 0 : yarnNodemanagerResourceMemoryMB.hashCode());
        hc = hc*31 + (dynamicAllocationEnabled == null ? 0 : dynamicAllocationEnabled.hashCode());
        hc = hc*31 + (executorInstances == null ? 0 : executorInstances.hashCode());
        hc = hc*31 + (executorCores == null ? 0 : executorCores.hashCode());
        hc = hc*31 + (executorMemory == null ? 0 : executorMemory.hashCode());
        hc = hc*31 + (dynamicAllocationMaxExecutors == null ? 0 : dynamicAllocationMaxExecutors.hashCode());
        hc = hc*31 + (executorMemoryOverhead == null ? 0 : executorMemoryOverhead.hashCode());
        hc = hc*31 + (yarnExecutorMemoryOverhead == null ? 0 : yarnExecutorMemoryOverhead.hashCode());
        return hc;
    }

    private boolean stringEquals(String string1, String string2) {
        if (string1 == null)
            return string2 == null;
        else if (string2 == null)
            return false;
        return string1.equals(string2);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(numNodes);

        writeNullableString(yarnNodemanagerResourceMemoryMB, out);

        writeNullableString(dynamicAllocationEnabled, out);
        writeNullableString(executorInstances, out);
        writeNullableString(executorCores, out);
        writeNullableString(executorMemory, out);
        writeNullableString(dynamicAllocationMaxExecutors, out);
        writeNullableString(executorMemoryOverhead, out);
        writeNullableString(yarnExecutorMemoryOverhead, out);

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        numNodes = in.readInt();

        yarnNodemanagerResourceMemoryMB = readNullableString(in);

        dynamicAllocationEnabled = readNullableString(in);
        executorInstances = readNullableString(in);
        executorCores = readNullableString(in);
        executorMemory = readNullableString(in);
        dynamicAllocationMaxExecutors = readNullableString(in);
        executorMemoryOverhead = readNullableString(in);
        yarnExecutorMemoryOverhead = readNullableString(in);
    }

    private static void writeNullableString(String value, ObjectOutput out) throws IOException {
        if (value != null) {
            out.writeBoolean(true);
            out.writeUTF(value);
        } else {
            out.writeBoolean(false);
        }
    }

    private static String readNullableString(ObjectInput in) throws IOException{
        if(in.readBoolean())
            return in.readUTF();
        return null;
    }

    public int getNumNodes() {
        return numNodes;
    }

    public void setNumNodes(int numNodes) {
        this.numNodes = numNodes;
    }

    public String getYarnNodemanagerResourceMemoryMB() {
        return yarnNodemanagerResourceMemoryMB;
    }

    public void setYarnNodemanagerResourceMemoryMB(String yarnNodemanagerResourceMemoryMB) {
        this.yarnNodemanagerResourceMemoryMB = yarnNodemanagerResourceMemoryMB;
    }

    public String getDynamicAllocationEnabled() {
        return dynamicAllocationEnabled;
    }

    public void setDynamicAllocationEnabled(String dynamicAllocationEnabled) {
        this.dynamicAllocationEnabled = dynamicAllocationEnabled;
    }

    public String getExecutorInstances() {
        return executorInstances;
    }

    public void setExecutorInstances(String executorInstances) {
        this.executorInstances = executorInstances;
    }

    public String getExecutorCores() {
        return executorCores;
    }

    public void setExecutorCores(String executorCores) {
        this.executorCores = executorCores;
    }

    public String getExecutorMemory() {
        return executorMemory;
    }

    public void setExecutorMemory(String executorMemory) {
        this.executorMemory = executorMemory;
    }

    public String getDynamicAllocationMaxExecutors() {
        return dynamicAllocationMaxExecutors;
    }

    public void setDynamicAllocationMaxExecutors(String dynamicAllocationMaxExecutors) {
        this.dynamicAllocationMaxExecutors = dynamicAllocationMaxExecutors;
    }

    public String getExecutorMemoryOverhead() {
        return executorMemoryOverhead;
    }

    public void setExecutorMemoryOverhead(String executorMemoryOverhead) {
        this.executorMemoryOverhead = executorMemoryOverhead;
    }

    public String getYarnExecutorMemoryOverhead() {
        return yarnExecutorMemoryOverhead;
    }

    public void setYarnExecutorMemoryOverhead(String yarnExecutorMemoryOverhead) {
        this.yarnExecutorMemoryOverhead = yarnExecutorMemoryOverhead;
    }

}
