package com.ir.hive.test;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.service.HiveClient;
import org.apache.hadoop.hive.service.HiveInterface;
import org.apache.hadoop.hive.service.HiveServer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class ServiceHive {

	  public TTransport transport;
	  public HiveInterface client;

	  public ServiceHive() throws MetaException {
	    client = new HiveServer.HiveServerHandler();
	  }

	  public ServiceHive(String host, int port) throws TTransportException {
	    transport = new TSocket(host, port);
	    TProtocol protocol = new TBinaryProtocol(transport);
	    client = new HiveClient(protocol);
	    transport.open();
	  }
	}