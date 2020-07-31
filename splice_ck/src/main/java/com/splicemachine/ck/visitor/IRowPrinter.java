package com.splicemachine.ck.visitor;

import org.apache.hadoop.hbase.client.Result;

import java.util.List;

public interface IRowPrinter {
    List<String> ProcessRow(Result row) throws Exception;
}
