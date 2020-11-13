package com.splicemachine.si.impl.server;

import com.splicemachine.hbase.CellUtils;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.TxnTestUtils;
import com.splicemachine.storage.CellType;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class SICompactionStateMutateTest {
    private long watermark = 1000;
    private SICompactionStateMutate cutNoPurge = new SICompactionStateMutate(
            new PurgeConfigBuilder().noPurge().build(), watermark);
    private SICompactionStateMutate cutForcePurge = new SICompactionStateMutate(
            new PurgeConfigBuilder().forcePurgeDeletes().purgeUpdates(true).build(), watermark);
    private SICompactionStateMutate cutPurgeDuringFlush = new SICompactionStateMutate(
            new PurgeConfigBuilder().purgeDeletesDuringFlush().purgeUpdates(true).build(), watermark);
    private SICompactionStateMutate cutPurgeDuringMajorCompaction = new SICompactionStateMutate(
            new PurgeConfigBuilder().purgeDeletesDuringMajorCompaction().purgeUpdates(true).build(), watermark);
    private SICompactionStateMutate cutPurgeDuringMinorCompaction = new SICompactionStateMutate(
            new PurgeConfigBuilder().purgeDeletesDuringMinorCompaction().purgeUpdates(true).build(), watermark);

    private List<Cell> inputCells = new ArrayList<>();
    private List<TxnView> transactions = new ArrayList<>();
    private List<Cell> outputCells = new ArrayList<>();

    private List<Cell> getNewlyAddedCells(List<Cell> inputCells, List<Cell> outputCells) {
        Set<Cell> set = new HashSet<>(outputCells);
        set.removeAll(new HashSet<>(inputCells));
        return new ArrayList<>(set);
    }

    private List<Cell> getRemovedCells(List<Cell> inputCells, List<Cell> outputCells) {
        return getNewlyAddedCells(outputCells, inputCells);
    }

    @Test
    public void mutateEmpty() throws IOException {
        SICompactionStateMutate.mutate(cutNoPurge, inputCells, transactions, outputCells);
        assertThat(outputCells, is(empty()));
    }

    @Test
    public void mutateSimpleOneActiveTransaction() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockValueCell(200),
                SITestUtils.getMockValueCell(100)));
        transactions.addAll(Arrays.asList(
                TxnTestUtils.getMockActiveTxn(200),
                TxnTestUtils.getMockActiveTxn(100)
        ));
        SICompactionStateMutate.mutate(cutNoPurge, inputCells, transactions, outputCells);
        assertThat(outputCells, equalTo(inputCells));
    }

    @Test
    public void mutateSimpleOneCommittedTransaction() throws IOException {
        inputCells.addAll(Collections.singletonList(
                SITestUtils.getMockValueCell(100)));
        transactions.addAll(Collections.singletonList(
                TxnTestUtils.getMockCommittedTxn(100, 200)));
        SICompactionStateMutate.mutate(cutNoPurge, inputCells, transactions, outputCells);
        assertThat(outputCells, hasSize(inputCells.size() + 1));
        List<Cell> newCells = getNewlyAddedCells(inputCells, outputCells);
        assertThat(newCells, hasSize(1));
        Cell commit = newCells.get(0);
        assertThat(CellUtils.getKeyValueType(commit), equalTo(CellType.COMMIT_TIMESTAMP));
        assertThat(commit.getTimestamp(), equalTo(100L));
        assertThat(Bytes.toLong(commit.getValueArray(), commit.getValueOffset(), commit.getValueLength()), equalTo(200L));
    }

    @Test
    public void mutateRolledBackTransaction() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockValueCell(300),
                SITestUtils.getMockValueCell(200),
                SITestUtils.getMockValueCell(100)
        ));
        transactions.addAll(Arrays.asList(
                TxnTestUtils.getMockRolledBackTxn(300),
                TxnTestUtils.getMockRolledBackTxn(200),
                TxnTestUtils.getMockRolledBackTxn(100)
        ));
        SICompactionStateMutate.mutate(cutNoPurge, inputCells, transactions, outputCells);
        assertThat(outputCells, is(empty()));
    }

    String toString(Collection<Cell> cells, boolean printUserData)
    {
        StringBuilder sb = new StringBuilder();
        int i=0;
        for(Cell c : cells)
        {
            sb.append(i++ + ": ");
            CellType ct = CellUtils.getKeyValueType(c);
            sb.append( ct.toString() );
            sb.append(" timestamp " + c.getTimestamp());
            if( ct == CellType.COMMIT_TIMESTAMP )
                sb.append(" commit " + CellUtils.getCommitTimestamp(c));
            else if( printUserData && ct == CellType.USER_DATA )
                sb.append("hex = " + CellUtils.getUserDataHex(c));
            sb.append("\n");
        }
        return sb.toString();
    }

    @Test
    public void mutateMixOfTransactions() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockValueCell(300),
                SITestUtils.getMockValueCell(200),
                SITestUtils.getMockValueCell(100)
        ));
        Assert.assertEquals( "0: USER_DATA timestamp 300\n" +
                "1: USER_DATA timestamp 200\n" +
                "2: USER_DATA timestamp 100\n", toString(inputCells, false) );

        transactions.addAll(Arrays.asList(
                TxnTestUtils.getMockCommittedTxn(300, 310),
                TxnTestUtils.getMockActiveTxn(200),
                TxnTestUtils.getMockRolledBackTxn(100)
        ));
        SICompactionStateMutate.mutate(cutNoPurge, inputCells, transactions, outputCells);

        Assert.assertEquals( "0: COMMIT_TIMESTAMP timestamp 300 commit 310\n" +
                "1: USER_DATA timestamp 300\n" +
                "2: USER_DATA timestamp 200\n", toString(outputCells, false) );

        assertThat(outputCells, hasSize(inputCells.size())); // + 1 commit, -1 rolled back
        List<Cell> newCells = getNewlyAddedCells(inputCells, outputCells);
        List<Cell> removedCells = getRemovedCells(inputCells, outputCells);
        assertThat(newCells, hasSize(1));
        assertThat(removedCells, hasSize(1));
        assertThat(removedCells.get(0).getTimestamp(), equalTo(100L));
        assertThat(newCells.get(0).getTimestamp(), equalTo(300L));
    }

    @Test
    public void mutateNullTransaction() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockValueCell(100),
                SITestUtils.getMockValueCell(100)
        ));
        transactions.addAll(Arrays.asList(
                null,
                null
        ));
        SICompactionStateMutate.mutate(cutNoPurge, inputCells, transactions, outputCells);
        assertThat(outputCells, equalTo(outputCells));
    }

    @Test
    public void mutateTombstones() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockTombstoneCell(200),
                SITestUtils.getMockValueCell(100)
        ));
        transactions.addAll(Arrays.asList(
                TxnTestUtils.getMockCommittedTxn(100, 250),
                TxnTestUtils.getMockCommittedTxn(100, 250)
        ));
        SICompactionStateMutate.mutate(cutNoPurge, inputCells, transactions, outputCells);
        assertThat(getRemovedCells(inputCells, outputCells), is(empty()));
    }

    @Test
    public void mutatePurgeTombstones() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(200, 210),
                SITestUtils.getMockCommitCell(100, 110),
                SITestUtils.getMockTombstoneCell(200),
                SITestUtils.getMockValueCell(100)
        ));
        transactions.addAll(Arrays.asList(
                null,
                null,
                TxnTestUtils.getMockCommittedTxn(200, 210),
                TxnTestUtils.getMockCommittedTxn(100, 110)
        ));
        SICompactionStateMutate.mutate(cutForcePurge, inputCells, transactions, outputCells);
        assertThat(outputCells, is(empty()));
    }

    @Test
    public void mutateDoNotPurgeBecauseTombstoneNotCommitted() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(100, 110),
                SITestUtils.getMockTombstoneCell(200),
                SITestUtils.getMockValueCell(100)
        ));
        transactions.addAll(Arrays.asList(
                null,
                TxnTestUtils.getMockActiveTxn(200),
                TxnTestUtils.getMockCommittedTxn(100, 110)
        ));
        SICompactionStateMutate.mutate(cutForcePurge, inputCells, transactions, outputCells);
        assertThat(outputCells, equalTo(inputCells));
    }

    @Test
    public void mutateDoNotPurgeBecauseTombstoneRolledBack() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(100, 110),
                SITestUtils.getMockTombstoneCell(200),
                SITestUtils.getMockValueCell(100)
        ));
        transactions.addAll(Arrays.asList(
                null,
                TxnTestUtils.getMockRolledBackTxn(200),
                TxnTestUtils.getMockCommittedTxn(100, 110)
        ));
        SICompactionStateMutate.mutate(cutForcePurge, inputCells, transactions, outputCells);
        assertThat(outputCells, hasSize(2));
        assertThat(outputCells, contains(
                SITestUtils.getMockCommitCell(100, 110),
                SITestUtils.getMockValueCell(100)));
    }

    @Test
    public void mutatePartialPurge() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(300, 310),
                SITestUtils.getMockCommitCell(200, 210),
                SITestUtils.getMockCommitCell(100, 110),
                SITestUtils.getMockAntiTombstoneCell(300),
                SITestUtils.getMockTombstoneCell(200),
                SITestUtils.getMockValueCell(300),
                SITestUtils.getMockValueCell(100)
        ));
        TxnView transaction1 = TxnTestUtils.getMockCommittedTxn(100, 110);
        TxnView transaction2 = TxnTestUtils.getMockCommittedTxn(200, 210);
        TxnView transaction3 = TxnTestUtils.getMockCommittedTxn(300, 310);

        transactions.addAll(Arrays.asList(
                null,
                null,
                null,
                transaction3,
                transaction2,
                transaction3,
                transaction1
        ));
        SICompactionStateMutate.mutate(cutForcePurge, inputCells, transactions, outputCells);
        assertThat(outputCells, hasSize(3));
        assertThat(outputCells, contains(
                SITestUtils.getMockCommitCell(300, 310),
                SITestUtils.getMockAntiTombstoneCell(300),
                SITestUtils.getMockValueCell(300)));
    }

    @Test
    public void mutatePurgeKeepTombstones() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(200, 210),
                SITestUtils.getMockCommitCell(100, 110),
                SITestUtils.getMockTombstoneCell(200),
                SITestUtils.getMockValueCell(100)
        ));
        transactions.addAll(Arrays.asList(
                null,
                null,
                TxnTestUtils.getMockCommittedTxn(200, 210),
                TxnTestUtils.getMockCommittedTxn(100, 110)
        ));
        SICompactionStateMutate.mutate(cutPurgeDuringFlush, inputCells, transactions, outputCells);
        assertThat(outputCells, hasSize(2));
        assertThat(getRemovedCells(inputCells, outputCells), containsInAnyOrder(
                SITestUtils.getMockCommitCell(100, 110),
                SITestUtils.getMockValueCell(100)
        ));
    }

    @Test
    public void mutateNonForcePurge() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(200, 210),
                SITestUtils.getMockCommitCell(100, 110),
                SITestUtils.getMockTombstoneCell(200),
                SITestUtils.getMockValueCell(100)
        ));
        transactions.addAll(Arrays.asList(
                null,
                null,
                TxnTestUtils.getMockCommittedTxn(200, 210),
                TxnTestUtils.getMockCommittedTxn(100, 110)
        ));
        SICompactionStateMutate.mutate(cutPurgeDuringMajorCompaction, inputCells, transactions, outputCells);
        assertThat(outputCells, is(empty()));
    }

    @Test
    public void mutatePurgeConsideringWatermark() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(1100, 1110),
                SITestUtils.getMockCommitCell(300, 310),
                SITestUtils.getMockCommitCell(200, 210),
                SITestUtils.getMockCommitCell(100, 110),
                SITestUtils.getMockTombstoneCell(1100),
                SITestUtils.getMockAntiTombstoneCell(300),
                SITestUtils.getMockTombstoneCell(200),
                SITestUtils.getMockValueCell(300),
                SITestUtils.getMockValueCell(100)
        ));

        TxnView transaction1 = TxnTestUtils.getMockCommittedTxn(100, 110);
        TxnView transaction2 = TxnTestUtils.getMockCommittedTxn(200, 210);
        TxnView transaction3 = TxnTestUtils.getMockCommittedTxn(300, 310);
        TxnView transaction4 = TxnTestUtils.getMockCommittedTxn(1100, 1110);
        transactions.addAll(Arrays.asList(
                null,
                null,
                null,
                null,
                transaction4,
                transaction3,
                transaction2,
                transaction3,
                transaction1
        ));

        SICompactionStateMutate.mutate(cutPurgeDuringMajorCompaction, inputCells, transactions, outputCells);
        assertThat(outputCells, hasSize(5));
        assertThat(getRemovedCells(inputCells, outputCells), containsInAnyOrder(
                SITestUtils.getMockValueCell(100),
                SITestUtils.getMockTombstoneCell(200),
                SITestUtils.getMockCommitCell(100, 110),
                SITestUtils.getMockCommitCell(200, 210)
        ));
    }

    @Test
    public void mutatePurgeConsideringWatermarkKeepTombstone() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(1100, 1110),
                SITestUtils.getMockCommitCell(300, 310),
                SITestUtils.getMockCommitCell(200, 210),
                SITestUtils.getMockCommitCell(100, 110),
                SITestUtils.getMockTombstoneCell(1100),
                SITestUtils.getMockAntiTombstoneCell(300),
                SITestUtils.getMockTombstoneCell(200),
                SITestUtils.getMockValueCell(300),
                SITestUtils.getMockValueCell(100)
        ));

        TxnView transaction1 = TxnTestUtils.getMockCommittedTxn(100, 110);
        TxnView transaction2 = TxnTestUtils.getMockCommittedTxn(200, 210);
        TxnView transaction3 = TxnTestUtils.getMockCommittedTxn(300, 310);
        TxnView transaction4 = TxnTestUtils.getMockCommittedTxn(1100, 1110);
        transactions.addAll(Arrays.asList(
                null,
                null,
                null,
                null,
                transaction4,
                transaction3,
                transaction2,
                transaction3,
                transaction1
        ));

        SICompactionStateMutate.mutate(cutPurgeDuringFlush, inputCells, transactions, outputCells);
        assertThat(outputCells, hasSize(7));
        assertThat(getRemovedCells(inputCells, outputCells), containsInAnyOrder(
                SITestUtils.getMockValueCell(100),
                SITestUtils.getMockCommitCell(100, 110)
        ));
    }

    @Test
    public void mutateDoNotPurgeBecauseDeleteStartedBeforeWatermarkButCommittedAfter() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(900, 1100),
                SITestUtils.getMockCommitCell(100, 110),
                SITestUtils.getMockTombstoneCell(900),
                SITestUtils.getMockValueCell(100)
        ));
        transactions.addAll(Arrays.asList(
                null,
                null,
                TxnTestUtils.getMockCommittedTxn(900, 1100),
                TxnTestUtils.getMockCommittedTxn(100, 110)
        ));
        SICompactionStateMutate.mutate(cutPurgeDuringMajorCompaction, inputCells, transactions, outputCells);
        assertThat(outputCells, equalTo(inputCells));

    }

    @Test
    public void mutatePurgeKeepTombstonesOverriddenByFirstOccurrenceToken() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(200, 210),
                SITestUtils.getMockCommitCell(100, 110),
                SITestUtils.getMockTombstoneCell(200),
                SITestUtils.getMockValueCell(100),
                SITestUtils.getMockFirstWriteCell(100)
        ));
        transactions.addAll(Arrays.asList(
                null,
                null,
                TxnTestUtils.getMockCommittedTxn(200, 210),
                TxnTestUtils.getMockCommittedTxn(100, 110),
                TxnTestUtils.getMockCommittedTxn(100, 110)
        ));
        SICompactionStateMutate.mutate(cutPurgeDuringFlush, inputCells, transactions, outputCells);
        assertThat(outputCells, is(empty()));
    }

    @Test
    public void mutatePurgeLatestTombstoneInMinorCompaction() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(200, 210),
                SITestUtils.getMockCommitCell(100, 110),
                SITestUtils.getMockTombstoneCell(200),
                SITestUtils.getMockValueCell(100),
                SITestUtils.getMockDeleteRightAfterFirstWriteCell(200),
                SITestUtils.getMockFirstWriteCell(100)
        ));

        System.out.println(toString(inputCells, false));

        TxnView transaction1 = TxnTestUtils.getMockCommittedTxn(100, 110);
        TxnView transaction2 = TxnTestUtils.getMockCommittedTxn(200, 210);
        transactions.addAll(Arrays.asList(
                null,
                null,
                transaction2,
                transaction1,
                transaction2,
                transaction1
        ));
        SICompactionStateMutate.mutate(cutPurgeDuringMinorCompaction, inputCells, transactions, outputCells);
        assertThat(outputCells, is(empty()));
        System.out.println(toString(outputCells, false));
    }

    @Test
    public void mutateDoNotPurgeLatestTombstoneInMinorCompactionDespiteTwoTokens() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(400, 410),
                SITestUtils.getMockCommitCell(300, 310),
                SITestUtils.getMockCommitCell(200, 210),
                SITestUtils.getMockCommitCell(100, 110),
                SITestUtils.getMockTombstoneCell(400),
                SITestUtils.getMockAntiTombstoneCell(300),
                SITestUtils.getMockTombstoneCell(200),
                SITestUtils.getMockValueCell(300),
                SITestUtils.getMockValueCell(100),
                SITestUtils.getMockDeleteRightAfterFirstWriteCell(200),
                SITestUtils.getMockFirstWriteCell(100)
        ));
        TxnView transaction1 = TxnTestUtils.getMockCommittedTxn(100, 110);
        TxnView transaction2 = TxnTestUtils.getMockCommittedTxn(200, 210);
        TxnView transaction3 = TxnTestUtils.getMockCommittedTxn(300, 310);
        TxnView transaction4 = TxnTestUtils.getMockCommittedTxn(400, 410);
        transactions.addAll(Arrays.asList(
                null,
                null,
                null,
                null,
                transaction4,
                transaction3,
                transaction2,
                transaction3,
                transaction1,
                transaction2,
                transaction1
        ));
        SICompactionStateMutate.mutate(cutPurgeDuringMinorCompaction, inputCells, transactions, outputCells);
        assertThat(outputCells, contains(
                SITestUtils.getMockCommitCell(400, 410),
                SITestUtils.getMockTombstoneCell(400)
        ));
    }

    @Test
    public void mutateDoNotPurgeLatestTombstoneDuringMinorCompactionMissingFirstWriteToken() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(200, 210),
                SITestUtils.getMockCommitCell(100, 110),
                SITestUtils.getMockTombstoneCell(200),
                SITestUtils.getMockValueCell(100),
                SITestUtils.getMockDeleteRightAfterFirstWriteCell(200)
        ));
        TxnView transaction1 = TxnTestUtils.getMockCommittedTxn(100, 110);
        TxnView transaction2 = TxnTestUtils.getMockCommittedTxn(200, 210);
        transactions.addAll(Arrays.asList(
                null,
                null,
                transaction2,
                transaction1,
                transaction2
        ));
        SICompactionStateMutate.mutate(cutPurgeDuringMinorCompaction, inputCells, transactions, outputCells);
        assertThat(outputCells, hasSize(3));
    }

    @Test
    public void mutateDoNotPurgeLatestTombstoneDuringMinorCompactionMissingDeleteToken() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(200, 210),
                SITestUtils.getMockCommitCell(100, 110),
                SITestUtils.getMockTombstoneCell(200),
                SITestUtils.getMockValueCell(100),
                SITestUtils.getMockFirstWriteCell(100)
        ));
        System.out.println(toString(inputCells, false));

        TxnView transaction1 = TxnTestUtils.getMockCommittedTxn(100, 110);
        TxnView transaction2 = TxnTestUtils.getMockCommittedTxn(200, 210);
        transactions.addAll(Arrays.asList(
                null,
                null,
                transaction2,
                transaction1,
                transaction1
        ));

        // minor compaction will leave commit timestamp and tombstone
        SICompactionStateMutate.mutate(cutPurgeDuringMinorCompaction, inputCells, transactions, outputCells);
        assertThat(outputCells, hasSize(2));
        System.out.println(toString(outputCells, false));

        // major compaction will delete tombstone
        SICompactionStateMutate.mutate(cutPurgeDuringMajorCompaction, inputCells, transactions, outputCells);
        assertThat(outputCells, hasSize(0));

    }

    @Test
    public void mutatePurgeAntiTombstoneGhostsTombstone() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(200, 210),
                SITestUtils.getMockCommitCell(100, 110),
                SITestUtils.getMockAntiTombstoneCell(200),
                SITestUtils.getMockValueCell(200, new boolean[]{true, false, false}),
                SITestUtils.getMockValueCell(100, new boolean[]{false, true, false}),
                SITestUtils.getMockDeleteRightAfterFirstWriteCell(200),
                SITestUtils.getMockFirstWriteCell(100)
        ));
        TxnView transaction1 = TxnTestUtils.getMockCommittedTxn(100, 110);
        TxnView transaction2 = TxnTestUtils.getMockCommittedTxn(200, 210);
        transactions.addAll(Arrays.asList(
                null,
                null,
                transaction2,
                transaction2,
                transaction1,
                transaction2,
                transaction1
        ));
        SICompactionStateMutate.mutate(cutPurgeDuringMinorCompaction, inputCells, transactions, outputCells);
        assertThat(outputCells, equalTo(inputCells));

        SICompactionStateMutate.mutate(cutPurgeDuringMajorCompaction, inputCells, transactions, outputCells);
        assertThat(outputCells, equalTo(inputCells));
    }

    @Test
    public void mutatePurgeTombstoneRespectBeginTimestampDespiteEverythingHavingTheSameCommitTimestamp() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockAntiTombstoneCell(300),
                SITestUtils.getMockTombstoneCell(200),
                SITestUtils.getMockValueCell(300),
                SITestUtils.getMockValueCell(100)
        ));
        System.out.println(toString(inputCells, false));
        TxnView transaction = TxnTestUtils.getMockCommittedTxn(50, 400);
        transactions.addAll(Arrays.asList(
                transaction,
                transaction,
                transaction,
                transaction
        ));
        SICompactionStateMutate.mutate(cutForcePurge, inputCells, transactions, outputCells);
        System.out.println(toString(outputCells, false));
        assertThat(outputCells, not(empty()));
        assertThat(outputCells, contains(
                SITestUtils.getMockCommitCell(300, 400),
                SITestUtils.getMockAntiTombstoneCell(300),
                SITestUtils.getMockValueCell(300)
        ));
    }

    @Test
    public void doNotPurgeTombstoneBecausePreviousAntiTombstoneHasSameTimestamp() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockAntiTombstoneCell(200),
                SITestUtils.getMockTombstoneCell(200),
                SITestUtils.getMockValueCell(200),
                SITestUtils.getMockValueCell(100)
        ));
        TxnView transaction = TxnTestUtils.getMockCommittedTxn(50, 400);
        transactions.addAll(Arrays.asList(
                transaction,
                transaction,
                transaction,
                transaction
        ));
        SICompactionStateMutate.mutate(cutForcePurge, inputCells, transactions, outputCells);
        assertThat(outputCells, not(empty()));
        assertThat(outputCells, contains(
                SITestUtils.getMockCommitCell(200, 400),
                SITestUtils.getMockAntiTombstoneCell(200),
                SITestUtils.getMockTombstoneCell(200),
                SITestUtils.getMockValueCell(200)
        ));
    }

    @Test
    public void doNotPurgeTombstoneBecauseNextAntiTombstoneHasSameTimestamp() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockTombstoneCell(200),
                SITestUtils.getMockAntiTombstoneCell(200),
                SITestUtils.getMockValueCell(200),
                SITestUtils.getMockValueCell(100)
        ));
        TxnView transaction = TxnTestUtils.getMockCommittedTxn(50, 400);
        transactions.addAll(Arrays.asList(
                transaction,
                transaction,
                transaction,
                transaction
        ));
        SICompactionStateMutate.mutate(cutForcePurge, inputCells, transactions, outputCells);
        assertThat(outputCells, not(empty()));
        assertThat(outputCells, contains(
                SITestUtils.getMockCommitCell(200, 400),
                SITestUtils.getMockAntiTombstoneCell(200),
                SITestUtils.getMockTombstoneCell(200),
                SITestUtils.getMockValueCell(200)
        ));
    }

    @Test
    public void mutateCannotPurgeNonGhostingUpdates() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(300, 310),
                SITestUtils.getMockCommitCell(200, 210),
                SITestUtils.getMockCommitCell(100, 110),
                SITestUtils.getMockValueCell(300, new boolean[]{false, true, false}),
                SITestUtils.getMockValueCell(200, new boolean[]{true, false, false}),
                SITestUtils.getMockValueCell(100, new boolean[]{true, true, true})
        ));
        transactions.addAll(Arrays.asList(
                null,
                null,
                null,
                TxnTestUtils.getMockCommittedTxn(300, 310),
                TxnTestUtils.getMockCommittedTxn(200, 210),
                TxnTestUtils.getMockCommittedTxn(100, 110)
        ));
        SICompactionStateMutate.mutate(cutForcePurge, inputCells, transactions, outputCells);
        assertThat(outputCells, equalTo(inputCells));
    }

    @Test
    public void mutatePurgeOneColumnUpdatedOverAndOver() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(300, 310),
                SITestUtils.getMockCommitCell(200, 210),
                SITestUtils.getMockCommitCell(100, 110),
                SITestUtils.getMockValueCell(300, new boolean[]{true}),
                SITestUtils.getMockValueCell(200, new boolean[]{true}),
                SITestUtils.getMockValueCell(100, new boolean[]{true})
        ));
        transactions.addAll(Arrays.asList(
                null,
                null,
                null,
                TxnTestUtils.getMockCommittedTxn(300, 310),
                TxnTestUtils.getMockCommittedTxn(200, 210),
                TxnTestUtils.getMockCommittedTxn(100, 110)
        ));
        SICompactionStateMutate.mutate(cutForcePurge, inputCells, transactions, outputCells);
        assertThat(outputCells, hasSize(2));
        assertThat(outputCells, contains(
                SITestUtils.getMockCommitCell(300, 310),
                SITestUtils.getMockValueCell(300, new boolean[]{true})
        ));
    }

    @Test
    public void mutatePurgePartialUpdateButNotBaselineInsert() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(300, 310),
                SITestUtils.getMockCommitCell(200, 210),
                SITestUtils.getMockCommitCell(100, 110),
                SITestUtils.getMockValueCell(300, new boolean[]{true, false}),
                SITestUtils.getMockValueCell(200, new boolean[]{true, false}),
                SITestUtils.getMockValueCell(100, new boolean[]{true, true})
        ));
        transactions.addAll(Arrays.asList(
                null,
                null,
                null,
                TxnTestUtils.getMockCommittedTxn(300, 310),
                TxnTestUtils.getMockCommittedTxn(200, 210),
                TxnTestUtils.getMockCommittedTxn(100, 110)
        ));
        SICompactionStateMutate.mutate(cutForcePurge, inputCells, transactions, outputCells);
        assertThat(outputCells, hasSize(4));
        assertThat(getRemovedCells(inputCells, outputCells), containsInAnyOrder(
                SITestUtils.getMockValueCell(200, new boolean[]{true, false}),
                SITestUtils.getMockCommitCell(200, 210)
        ));
    }

    @Test
    public void mutatePurgeBaselineUpdateBecausePartialUpdatesCoverItAll() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(300, 310),
                SITestUtils.getMockCommitCell(200, 210),
                SITestUtils.getMockCommitCell(100, 110),
                SITestUtils.getMockValueCell(300, new boolean[]{false, true}),
                SITestUtils.getMockValueCell(200, new boolean[]{true, false}),
                SITestUtils.getMockValueCell(100, new boolean[]{true, true})
        ));
        transactions.addAll(Arrays.asList(
                null,
                null,
                null,
                TxnTestUtils.getMockCommittedTxn(300, 310),
                TxnTestUtils.getMockCommittedTxn(200, 210),
                TxnTestUtils.getMockCommittedTxn(100, 110)
        ));
        SICompactionStateMutate.mutate(cutForcePurge, inputCells, transactions, outputCells);
        assertThat(outputCells, hasSize(4));
        assertThat(getRemovedCells(inputCells, outputCells), containsInAnyOrder(
                SITestUtils.getMockValueCell(100, new boolean[]{true, true}),
                SITestUtils.getMockCommitCell(100, 110)
        ));
    }

    @Test
    public void mutateDoNotPurgeUpdateBecauseOverrideIsAboveWatermark() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(1200, 1210),
                SITestUtils.getMockCommitCell(100, 110),
                SITestUtils.getMockValueCell(1200, new boolean[]{true}),
                SITestUtils.getMockValueCell(100, new boolean[]{true})
        ));
        transactions.addAll(Arrays.asList(
                null,
                null,
                TxnTestUtils.getMockCommittedTxn(1200, 1210),
                TxnTestUtils.getMockCommittedTxn(100, 110)
        ));
        SICompactionStateMutate.mutate(cutForcePurge, inputCells, transactions, outputCells);
        assertThat(outputCells, equalTo(inputCells));
    }

    @Test
    public void mutateDoNotPurgeOldUpdateBecauseOverrideStartedBeforeWatermarkButCommittedAfter() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(900, 1100),
                SITestUtils.getMockCommitCell(100, 110),
                SITestUtils.getMockValueCell(900),
                SITestUtils.getMockValueCell(100)
        ));
        transactions.addAll(Arrays.asList(
                null,
                null,
                TxnTestUtils.getMockCommittedTxn(900, 1100),
                TxnTestUtils.getMockCommittedTxn(100, 110)
        ));
        SICompactionStateMutate.mutate(cutForcePurge, inputCells, transactions, outputCells);
        assertThat(outputCells, equalTo(inputCells));
    }

    @Test
    public void mutatePurgeUpdatesPrimaryKeys() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(200, 210),
                SITestUtils.getMockCommitCell(100, 110),
                SITestUtils.getMockValueCell(200, new boolean[]{}),
                SITestUtils.getMockValueCell(100, new boolean[]{})
        ));
        transactions.addAll(Arrays.asList(
                null,
                null,
                TxnTestUtils.getMockCommittedTxn(200, 210),
                TxnTestUtils.getMockCommittedTxn(100, 110)
        ));
        SICompactionStateMutate.mutate(cutForcePurge, inputCells, transactions, outputCells);
        assertThat(outputCells, hasSize(2));
        assertThat(getRemovedCells(inputCells, outputCells), containsInAnyOrder(
                SITestUtils.getMockValueCell(100, new boolean[]{}),
                SITestUtils.getMockCommitCell(100, 110)
        ));
    }

    @Test
    public void mutateCalculatesSizeCorrectly() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(200, 210),
                SITestUtils.getMockCommitCell(100, 110),
                SITestUtils.getMockTombstoneCell(200),
                SITestUtils.getMockValueCell(100)
        ));
        transactions.addAll(Arrays.asList(
                null,
                null,
                TxnTestUtils.getMockCommittedTxn(200, 210),
                TxnTestUtils.getMockCommittedTxn(100, 110)
        ));
        long actualSize = SICompactionStateMutate.mutate(cutForcePurge, inputCells, transactions, outputCells);
        assertThat(actualSize, equalTo(SITestUtils.getSize(inputCells)));
    }
}
