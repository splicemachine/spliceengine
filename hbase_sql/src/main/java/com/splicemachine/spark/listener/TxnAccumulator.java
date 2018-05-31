package com.splicemachine.spark.listener;

import com.splicemachine.si.api.txn.TxnView;
import org.apache.spark.util.AccumulatorV2;

/**
 *
 *
 *
 */
public class TxnAccumulator extends AccumulatorV2<String,String> {
    //private TxnView txnView;
    private String txnView;
    public TxnAccumulator() {}

    public TxnAccumulator(String txnView) {
        this.txnView = txnView;
    }

    @Override
    public boolean isZero() {
        return true;
    }

    @Override
    public void merge(AccumulatorV2<String, String> other) {
        System.out.println("merge");
    }

    @Override
    public AccumulatorV2<String, String> copy() {
        return new TxnAccumulator(txnView);
    }

    @Override
    public String value() {
        return txnView;
    }

    @Override
    public void add(String v) {
        txnView = v;
    }

    @Override
    public void reset() {
        System.out.println("dd");
    }
}
