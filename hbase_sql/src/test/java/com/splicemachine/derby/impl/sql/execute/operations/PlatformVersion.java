package com.splicemachine.derby.impl.sql.execute.operations;

import java.util.Arrays;

class PlatformVersion {
    public int zookeeperVersion[];
    public String platform;
    public int ver[];

    private int[] parseVersion(String s) {
        return Arrays.stream(s.split("\\."))
                .mapToInt( str -> Integer.parseInt(str) ).toArray();
    }

    PlatformVersion()
    {
        org.apache.zookeeper.Version version = new org.apache.zookeeper.Version();
        String a[] = version.getVersion().split("-");
        zookeeperVersion = parseVersion(a[0]);
        platform = a[1].substring(0, 3);
        ver = parseVersion(a[1].substring(3));
    }

}