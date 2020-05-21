package com.splicemachine.olap;

import java.util.Objects;

public class OlapServerZNode implements Comparable<OlapServerZNode> {
    private String queueName;
    private int sequence;

    public OlapServerZNode(String queueName, int sequence) {
        this.queueName = queueName;
        this.sequence = sequence;
    }

    public String getQueueName() {
        return queueName;
    }

    public long getSequence() {
        return sequence;
    }

    public String toZNode() {
        return String.format("%s%010d", queueName, sequence);
    }

    public static OlapServerZNode parseFrom(String nodeName) {
        String sequence = nodeName.substring(nodeName.length() - 10);
        String queueName = nodeName.substring(0, nodeName.length() - 10);
        return new OlapServerZNode(queueName, Integer.parseInt(sequence));
    }

    @Override
    public int compareTo(OlapServerZNode o) {
        if (queueName.equals(o.queueName)) {
            return o.sequence - sequence;
        } else {
            return queueName.compareTo(o.queueName);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OlapServerZNode that = (OlapServerZNode) o;
        return sequence == that.sequence &&
                Objects.equals(queueName, that.queueName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queueName, sequence);
    }
}
