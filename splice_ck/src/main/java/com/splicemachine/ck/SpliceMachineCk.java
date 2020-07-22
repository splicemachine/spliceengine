package com.splicemachine.ck;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class SpliceMachineCk {

    public static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    public static final String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";

    public void usage() {
        System.out.println("-zkq [zookeeper quroum] -zkc [zookeeper client] <command>");
        System.out.println("available commands: ");
        System.out.println("print <region> <rowid>");
    }

    public Configuration createConfiguration(String[] args) {
        assert args.length >= 7;
        Configuration conf = HBaseConfiguration.create();
        conf.set(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, args[1]);
        conf.set(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, args[3]);
        return conf;
    }

    public static void main(String[] args) {
        // args: zkq, zkc, [command]
        SpliceMachineCk spliceMachineCk = new SpliceMachineCk();
        if(args.length < 7) {
           spliceMachineCk.usage();
        }
        Configuration c = spliceMachineCk.createConfiguration(args);
        RowInspector rowInspector = new RowInspector(c);
        System.out.println(rowInspector.scanRow(args[5], args[6]));
    }
}
