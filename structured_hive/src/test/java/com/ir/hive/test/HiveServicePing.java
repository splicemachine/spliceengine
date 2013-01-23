package com.ir.hive.test;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HiveServicePing extends Configured implements Tool {

	  public static void main(String[] args) throws Exception {
	    int ret = ToolRunner.run(new Configuration(), new HiveServicePing(), args);
	    System.exit(ret);
	  }

	  @Override
	  public int run(String[] args) throws Exception {
	    Configuration conf = getConf();
	    for (String arg : args) {
	      if (arg.contains("=")) {
	        String vname = arg.substring(0, arg.indexOf('='));
	        String vval = arg.substring(arg.indexOf('=') + 1);
	        conf.set(vname, vval.replace("\"", ""));
	      }
	    }
	    System.out.println(conf.get("service.host"));
	    System.out.println(conf.get("service.port"));
	    ServiceHive sh = new ServiceHive(conf.get("service.host"), conf.getInt("service.port", 10000));
	    List<String> tables = sh.client.get_all_tables("default");
	    for (String table : tables) {
	      System.out.println(table);
	    }
	    return 0;
	  }
	}
