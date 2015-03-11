package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.collect.Lists;
import com.splicemachine.derby.impl.sql.execute.operations.framework.GroupedRow;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.impl.sql.execute.operations.groupedaggregate.GroupedAggregateBuffer;
import com.splicemachine.derby.impl.sql.execute.operations.groupedaggregate.ScanGroupedAggregateIterator;
import com.splicemachine.derby.utils.StandardIterator;
import com.splicemachine.derby.utils.StandardIterators;
import com.splicemachine.derby.utils.StandardSupplier;
import com.splicemachine.derby.utils.marshall.BareKeyHash;
import com.splicemachine.derby.utils.marshall.KeyEncoder;
import com.splicemachine.derby.utils.marshall.NoOpPostfix;
import com.splicemachine.derby.utils.marshall.NoOpPrefix;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.metrics.Metrics;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecAggregator;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.iapi.types.UserType;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;


/**
 * @author Scott Fines
 * Created on: 11/2/13
 */
public class ScanGroupedAggregatorTest {


    @Test
    public void testGroupedAggregateWithRollups() throws Exception {
        List<ExecRow> sourceRows = Lists.newArrayListWithCapacity(10);
        final ExecRow template = new ValueRow(4);
        template.setRowArray(new DataValueDescriptor[]{
                new SQLInteger(),
                new SQLInteger(),
                new SQLInteger(),
                new UserType()
        });
        for(int i=0;i<10;i++){
            template.resetRowArray();
            template.getColumn(1).setValue(i);
            template.getColumn(2).setValue(i*2);
            sourceRows.add(template.getClone());
        }

        StandardIterator<ExecRow> source = StandardIterators.wrap(sourceRows);

        SpliceGenericAggregator countAggregator = GroupedAggregatorTest.countAggregator(4,1,3,false);
        GroupedAggregateBuffer buffer = new GroupedAggregateBuffer(20,
                new SpliceGenericAggregator[]{countAggregator},false,new StandardSupplier<ExecRow>() {
            @Override
            public ExecRow get() throws StandardException {
                return template.getNewNullRow();
            }
        },new WarningCollector() {
            @Override
            public void addWarning(String warningState) throws StandardException {
                Assert.fail("No warnings should be added!");
            }
        },false, Metrics.noOpMetricFactory(), true);

        int[] groupColumns = new int[]{0,1};
        boolean[] groupSortOrder = new boolean[]{true,true};
				DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(template);
				KeyEncoder encoder = new KeyEncoder(NoOpPrefix.INSTANCE, BareKeyHash.encoder(groupColumns,groupSortOrder,serializers), NoOpPostfix.INSTANCE);
        ScanGroupedAggregateIterator aggregator = new ScanGroupedAggregateIterator(buffer,
                source,encoder,groupColumns,true);

        List<GroupedRow> results = Lists.newArrayListWithExpectedSize(21);
        GroupedRow row = aggregator.next(null);
        while(row!=null){
            results.add(row.deepCopy());
            row = aggregator.next(null);
        }

        Assert.assertEquals("Incorrect number of grouped rows returned!",21,results.size());
        Collections.sort(results,new Comparator<GroupedRow>() {
            @Override
            public int compare(GroupedRow o1, GroupedRow o2) {
                return Bytes.compareTo(o1.getGroupingKey(), o2.getGroupingKey());
            }
        });

        for(int i=0;i<results.size();i++){
            GroupedRow groupedRow = results.get(i);
            ExecRow dataRow = groupedRow.getRow();
            byte[] groupKey = groupedRow.getGroupingKey();
            MultiFieldDecoder decoder = MultiFieldDecoder.wrap(groupKey);
            boolean allNull=true;
            for(int colPos=0;colPos<groupColumns.length;colPos++){
                if(decoder.nextIsNull()){
                    Assert.assertTrue("Column "+ colPos+" does not match the grouping key",dataRow.getColumn(colPos+1).isNull());
                }else{
                    allNull=false;
                    int next = decoder.decodeNextInt();
                    int actual = dataRow.getColumn(colPos+1).getInt();
                    Assert.assertEquals("incorrect grouping col",next,actual);
                }
            }
            if(allNull){
                int count = ((ExecAggregator)dataRow.getColumn(4).getObject()).getResult().getInt();
                Assert.assertEquals("Incorrect count column!",10,count);
            }else {
                int count = ((ExecAggregator)dataRow.getColumn(4).getObject()).getResult().getInt();
                Assert.assertEquals("Incorrect count column!",1,count);
            }


        }
    }

    @Test
    public void testGroupedAggregateWorksCorrectly() throws Exception {
        List<ExecRow> sourceRows = Lists.newArrayListWithCapacity(10);
        final ExecRow template = new ValueRow(3);
        template.setRowArray(new DataValueDescriptor[]{
                new SQLInteger(),
                new SQLInteger(),
                new UserType()
        });
        for(int i=0;i<10;i++){
            template.resetRowArray();
            template.getColumn(1).setValue(i);
            sourceRows.add(template.getClone());
        }

        SpliceGenericAggregator countAggregator = GroupedAggregatorTest.countAggregator(3,1,2,false);

        Map<String, List<GroupedRow>> results =
            GroupedAggregatorTest.runGroupedAggregateTest(sourceRows, countAggregator);


        List<GroupedRow> scannedRows = results.get("allScannedRows");
        Assert.assertEquals("Incorrect number of grouped rows returned!",10,scannedRows.size());

        Collections.sort(scannedRows,new Comparator<GroupedRow>() {
            @Override
            public int compare(GroupedRow o1, GroupedRow o2) {
                return Bytes.compareTo(o1.getGroupingKey(), o2.getGroupingKey());
            }
        });

        for(int i=0;i<10;i++){
            GroupedRow groupedRow = scannedRows.get(i);
            byte[] groupKey = groupedRow.getGroupingKey();
            Assert.assertEquals("incorrect grouping key!",i,Encoding.decodeInt(groupKey));
            ExecRow dataRow = groupedRow.getRow();
            Assert.assertEquals("Incorrect first column!",i,dataRow.getColumn(1).getInt());

            int count = ((ExecAggregator)dataRow.getColumn(3).getObject()).getResult().getInt();
            Assert.assertEquals("Incorrect count column!",1,count);
        }
    }
}
