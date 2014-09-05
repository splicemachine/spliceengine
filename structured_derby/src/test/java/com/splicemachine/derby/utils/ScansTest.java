package com.splicemachine.derby.utils;

import com.splicemachine.derby.impl.sql.execute.operations.SkippingScanFilter;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.junit.Test;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class ScansTest {

    @Test
    public void findSkippingScanFilter() {
        Scan scan = new Scan();
        SkippingScanFilter skippingScanFilter = new SkippingScanFilter(null, null);
        scan.setFilter(skippingScanFilter);

        assertSame(skippingScanFilter, Scans.findSkippingScanFilter(scan));
    }

    @Test
    public void findSkippingScanFilter_inList() {
        Scan scan = new Scan();
        SkippingScanFilter skippingScanFilter = new SkippingScanFilter(null, null);
        FilterList filterList = new FilterList();
        filterList.addFilter(skippingScanFilter);
        scan.setFilter(filterList);

        assertSame(skippingScanFilter, Scans.findSkippingScanFilter(scan));
    }

    @Test
    public void findSkippingScanFilter_returnsNull_whenSkippingScanFilterNotPresent() {
        Scan scan = new Scan();
        assertNull(Scans.findSkippingScanFilter(scan));
    }
}