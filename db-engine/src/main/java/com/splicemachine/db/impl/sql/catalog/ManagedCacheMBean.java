package com.splicemachine.db.impl.sql.catalog;
import javax.management.MXBean;

@MXBean
public interface ManagedCacheMBean <K, V> {

    long getSize();
    long getMissCount();
    double getMissRate();
    long getHitCount();
    double getHitRate();
    void invalidateAll();
    void put(K var1, V var2);
    V getIfPresent(K k);
    void invalidate(K k);
}
