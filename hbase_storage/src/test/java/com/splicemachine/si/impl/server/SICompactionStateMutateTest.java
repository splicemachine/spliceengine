package com.splicemachine.si.impl.server;

import com.splicemachine.hbase.CellUtils;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.TxnTestUtils;
import com.splicemachine.storage.CellType;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class SICompactionStateMutateTest {
    private long watermark = 0x1000;
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
        cutNoPurge.mutate(inputCells, transactions, outputCells);
        assertThat(outputCells, is(empty()));
    }

    @Test
    public void mutateSimpleOneActiveTransaction() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockValueCell(0x200),
                SITestUtils.getMockValueCell(0x100)));
        TxnView activeTransaction = TxnTestUtils.getMockActiveTxn(0x100);
        transactions.addAll(Arrays.asList(
                activeTransaction,
                activeTransaction));
        cutNoPurge.mutate(inputCells, transactions, outputCells);
        assertThat(outputCells, equalTo(inputCells));
    }

    @Test
    public void mutateSimpleOneCommittedTransaction() throws IOException {
        inputCells.addAll(Collections.singletonList(
                SITestUtils.getMockValueCell(0x100)));
        transactions.addAll(Collections.singletonList(
                TxnTestUtils.getMockCommittedTxn(0x100, 0x200)));
        cutNoPurge.mutate(inputCells, transactions, outputCells);
        assertThat(outputCells, hasSize(inputCells.size() + 1));
        List<Cell> newCells = getNewlyAddedCells(inputCells, outputCells);
        assertThat(newCells, hasSize(1));
        Cell commit = newCells.get(0);
        assertThat(CellUtils.getKeyValueType(commit), equalTo(CellType.COMMIT_TIMESTAMP));
        assertThat(commit.getTimestamp(), equalTo(0x100L));
        assertThat(Bytes.toLong(commit.getValueArray(), commit.getValueOffset(), commit.getValueLength()), equalTo(0x200L));
    }

    @Test
    public void mutateRolledBackTransaction() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockValueCell(0x300),
                SITestUtils.getMockValueCell(0x200),
                SITestUtils.getMockValueCell(0x100)
        ));
        TxnView rolledBackTransaction = TxnTestUtils.getMockRolledBackTxn(0x400);
        transactions.addAll(Arrays.asList(
                rolledBackTransaction,
                rolledBackTransaction,
                rolledBackTransaction
        ));
        cutNoPurge.mutate(inputCells, transactions, outputCells);
        assertThat(outputCells, is(empty()));
    }

    @Test
    public void mutateMixOfTransactions() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockValueCell(0x300),
                SITestUtils.getMockValueCell(0x200),
                SITestUtils.getMockValueCell(0x100)
        ));
        transactions.addAll(Arrays.asList(
                TxnTestUtils.getMockCommittedTxn(0x300, 0x310),
                TxnTestUtils.getMockActiveTxn(0x200),
                TxnTestUtils.getMockRolledBackTxn(0x100)
        ));
        cutNoPurge.mutate(inputCells, transactions, outputCells);
        assertThat(outputCells, hasSize(inputCells.size())); // + 1 commit, -1 rolled back
        List<Cell> newCells = getNewlyAddedCells(inputCells, outputCells);
        List<Cell> removedCells = getRemovedCells(inputCells, outputCells);
        assertThat(newCells, hasSize(1));
        assertThat(removedCells, hasSize(1));
        assertThat(removedCells.get(0).getTimestamp(), equalTo(0x100L));
        assertThat(newCells.get(0).getTimestamp(), equalTo(0x300L));
    }

    @Test
    public void mutateNullTransaction() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockValueCell(0x100),
                SITestUtils.getMockValueCell(0x100)
        ));
        transactions.addAll(Arrays.asList(
                null,
                null
        ));
        cutNoPurge.mutate(inputCells, transactions, outputCells);
        assertThat(outputCells, equalTo(outputCells));
    }

    @Test
    public void mutateTombstones() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockTombstoneCell(0x200),
                SITestUtils.getMockValueCell(0x100)
        ));
        TxnView committedTransaction = TxnTestUtils.getMockCommittedTxn(0x100, 0x250);
        transactions.addAll(Arrays.asList(
                committedTransaction,
                committedTransaction
        ));
        cutNoPurge.mutate(inputCells, transactions, outputCells);
        assertThat(getRemovedCells(inputCells, outputCells), is(empty()));
    }

    @Test
    public void mutatePurgeTombstones() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(0x200, 0x210),
                SITestUtils.getMockCommitCell(0x100, 0x110),
                SITestUtils.getMockTombstoneCell(0x200),
                SITestUtils.getMockValueCell(0x100)
        ));
        transactions.addAll(Arrays.asList(
                null,
                null,
                TxnTestUtils.getMockCommittedTxn(0x200, 0x210),
                TxnTestUtils.getMockCommittedTxn(0x100, 0x110)
        ));
        cutForcePurge.mutate(inputCells, transactions, outputCells);
        assertThat(outputCells, is(empty()));
    }

    @Test
    public void mutateDoNotPurgeBecauseTombstoneNotCommitted() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(0x100, 0x110),
                SITestUtils.getMockTombstoneCell(0x200),
                SITestUtils.getMockValueCell(0x100)
        ));
        transactions.addAll(Arrays.asList(
                null,
                TxnTestUtils.getMockActiveTxn(0x200),
                TxnTestUtils.getMockCommittedTxn(0x100, 0x110)
        ));
        cutForcePurge.mutate(inputCells, transactions, outputCells);
        assertThat(outputCells, equalTo(inputCells));
    }

    @Test
    public void mutateDoNotPurgeBecauseTombstoneRolledBack() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(0x100, 0x110),
                SITestUtils.getMockTombstoneCell(0x200),
                SITestUtils.getMockValueCell(0x100)
        ));
        transactions.addAll(Arrays.asList(
                null,
                TxnTestUtils.getMockRolledBackTxn(0x200),
                TxnTestUtils.getMockCommittedTxn(0x100, 0x110)
        ));
        cutForcePurge.mutate(inputCells, transactions, outputCells);
        assertThat(outputCells, hasSize(2));
        assertThat(outputCells, contains(
                SITestUtils.getMockCommitCell(0x100, 0x110),
                SITestUtils.getMockValueCell(0x100)));
    }

    @Test
    public void mutatePartialPurge() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(0x300, 0x310),
                SITestUtils.getMockCommitCell(0x200, 0x210),
                SITestUtils.getMockCommitCell(0x100, 0x110),
                SITestUtils.getMockAntiTombstoneCell(0x300),
                SITestUtils.getMockTombstoneCell(0x200),
                SITestUtils.getMockValueCell(0x300),
                SITestUtils.getMockValueCell(0x100)
        ));
        TxnView transaction1 = TxnTestUtils.getMockCommittedTxn(0x100, 0x110);
        TxnView transaction2 = TxnTestUtils.getMockCommittedTxn(0x200, 0x210);
        TxnView transaction3 = TxnTestUtils.getMockCommittedTxn(0x300, 0x310);

        transactions.addAll(Arrays.asList(
                null,
                null,
                null,
                transaction3,
                transaction2,
                transaction3,
                transaction1
        ));
        cutForcePurge.mutate(inputCells, transactions, outputCells);
        assertThat(outputCells, hasSize(3));
        assertThat(outputCells, contains(
                SITestUtils.getMockCommitCell(0x300, 0x310),
                SITestUtils.getMockAntiTombstoneCell(0x300),
                SITestUtils.getMockValueCell(0x300)));
    }

    @Test
    public void mutatePurgeKeepTombstones() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(0x200, 0x210),
                SITestUtils.getMockCommitCell(0x100, 0x110),
                SITestUtils.getMockTombstoneCell(0x200),
                SITestUtils.getMockValueCell(0x100)
        ));
        transactions.addAll(Arrays.asList(
                null,
                null,
                TxnTestUtils.getMockCommittedTxn(0x200, 0x210),
                TxnTestUtils.getMockCommittedTxn(0x100, 0x110)
        ));
        cutPurgeDuringFlush.mutate(inputCells, transactions, outputCells);
        assertThat(outputCells, hasSize(2));
        assertThat(getRemovedCells(inputCells, outputCells), containsInAnyOrder(
                SITestUtils.getMockCommitCell(0x100, 0x110),
                SITestUtils.getMockValueCell(0x100)
        ));
    }

    @Test
    public void mutateNonForcePurge() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(0x200, 0x210),
                SITestUtils.getMockCommitCell(0x100, 0x110),
                SITestUtils.getMockTombstoneCell(0x200),
                SITestUtils.getMockValueCell(0x100)
        ));
        transactions.addAll(Arrays.asList(
                null,
                null,
                TxnTestUtils.getMockCommittedTxn(0x200, 0x210),
                TxnTestUtils.getMockCommittedTxn(0x100, 0x110)
        ));
        cutPurgeDuringMajorCompaction.mutate(inputCells, transactions, outputCells);
        assertThat(outputCells, is(empty()));
    }

    @Test
    public void mutatePurgeConsideringWatermark() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(0x1100, 0x1110),
                SITestUtils.getMockCommitCell(0x300, 0x310),
                SITestUtils.getMockCommitCell(0x200, 0x210),
                SITestUtils.getMockCommitCell(0x100, 0x110),
                SITestUtils.getMockTombstoneCell(0x1100),
                SITestUtils.getMockAntiTombstoneCell(0x300),
                SITestUtils.getMockTombstoneCell(0x200),
                SITestUtils.getMockValueCell(0x300),
                SITestUtils.getMockValueCell(0x100)
        ));

        TxnView transaction1 = TxnTestUtils.getMockCommittedTxn(0x100, 0x110);
        TxnView transaction2 = TxnTestUtils.getMockCommittedTxn(0x200, 0x210);
        TxnView transaction3 = TxnTestUtils.getMockCommittedTxn(0x300, 0x310);
        TxnView transaction4 = TxnTestUtils.getMockCommittedTxn(0x1100, 0x1110);
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

        cutPurgeDuringMajorCompaction.mutate(inputCells, transactions, outputCells);
        assertThat(outputCells, hasSize(5));
        assertThat(getRemovedCells(inputCells, outputCells), containsInAnyOrder(
                SITestUtils.getMockValueCell(0x100),
                SITestUtils.getMockTombstoneCell(0x200),
                SITestUtils.getMockCommitCell(0x100, 0x110),
                SITestUtils.getMockCommitCell(0x200, 0x210)
        ));
    }

    @Test
    public void mutatePurgeConsideringWatermarkKeepTombstone() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(0x1100, 0x1110),
                SITestUtils.getMockCommitCell(0x300, 0x310),
                SITestUtils.getMockCommitCell(0x200, 0x210),
                SITestUtils.getMockCommitCell(0x100, 0x110),
                SITestUtils.getMockTombstoneCell(0x1100),
                SITestUtils.getMockAntiTombstoneCell(0x300),
                SITestUtils.getMockTombstoneCell(0x200),
                SITestUtils.getMockValueCell(0x300),
                SITestUtils.getMockValueCell(0x100)
        ));

        TxnView transaction1 = TxnTestUtils.getMockCommittedTxn(0x100, 0x110);
        TxnView transaction2 = TxnTestUtils.getMockCommittedTxn(0x200, 0x210);
        TxnView transaction3 = TxnTestUtils.getMockCommittedTxn(0x300, 0x310);
        TxnView transaction4 = TxnTestUtils.getMockCommittedTxn(0x1100, 0x1110);
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

        cutPurgeDuringFlush.mutate(inputCells, transactions, outputCells);
        assertThat(outputCells, hasSize(7));
        assertThat(getRemovedCells(inputCells, outputCells), containsInAnyOrder(
                SITestUtils.getMockValueCell(0x100),
                SITestUtils.getMockCommitCell(0x100, 0x110)
        ));
    }

    @Test
    public void mutateDoNotPurgeBecauseDeleteStartedBeforeWatermarkButCommittedAfter() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(0x900, 0x1100),
                SITestUtils.getMockCommitCell(0x100, 0x110),
                SITestUtils.getMockTombstoneCell(0x900),
                SITestUtils.getMockValueCell(0x100)
        ));
        transactions.addAll(Arrays.asList(
                null,
                null,
                TxnTestUtils.getMockCommittedTxn(0x900, 0x1100),
                TxnTestUtils.getMockCommittedTxn(0x100, 0x110)
        ));
        cutPurgeDuringMajorCompaction.mutate(inputCells, transactions, outputCells);
        assertThat(outputCells, equalTo(inputCells));

    }

    @Test
    public void mutatePurgeKeepTombstonesOverriddenByFirstOccurrenceToken() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(0x200, 0x210),
                SITestUtils.getMockCommitCell(0x100, 0x110),
                SITestUtils.getMockTombstoneCell(0x200),
                SITestUtils.getMockValueCell(0x100),
                SITestUtils.getMockFirstWriteCell(0x100)
        ));
        transactions.addAll(Arrays.asList(
                null,
                null,
                TxnTestUtils.getMockCommittedTxn(0x200, 0x210),
                TxnTestUtils.getMockCommittedTxn(0x100, 0x110),
                TxnTestUtils.getMockCommittedTxn(0x100, 0x110)
        ));
        cutPurgeDuringFlush.mutate(inputCells, transactions, outputCells);
        assertThat(outputCells, is(empty()));
    }

    @Test
    public void mutatePurgeLatestTombstoneInMinorCompaction() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(0x200, 0x210),
                SITestUtils.getMockCommitCell(0x100, 0x110),
                SITestUtils.getMockTombstoneCell(0x200),
                SITestUtils.getMockValueCell(0x100),
                SITestUtils.getMockDeleteRightAfterFirstWriteCell(0x200),
                SITestUtils.getMockFirstWriteCell(0x100)
        ));

        TxnView transaction1 = TxnTestUtils.getMockCommittedTxn(0x100, 0x110);
        TxnView transaction2 = TxnTestUtils.getMockCommittedTxn(0x200, 0x210);
        transactions.addAll(Arrays.asList(
                null,
                null,
                transaction2,
                transaction1,
                transaction2,
                transaction1
        ));
        cutPurgeDuringMinorCompaction.mutate(inputCells, transactions, outputCells);
        assertThat(outputCells, is(empty()));
    }

    @Test
    public void mutateDoNotPurgeLatestTombstoneInMinorCompactionDespiteTwoTokens() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(0x400, 0x410),
                SITestUtils.getMockCommitCell(0x300, 0x310),
                SITestUtils.getMockCommitCell(0x200, 0x210),
                SITestUtils.getMockCommitCell(0x100, 0x110),
                SITestUtils.getMockTombstoneCell(0x400),
                SITestUtils.getMockAntiTombstoneCell(0x300),
                SITestUtils.getMockTombstoneCell(0x200),
                SITestUtils.getMockValueCell(0x300),
                SITestUtils.getMockValueCell(0x100),
                SITestUtils.getMockDeleteRightAfterFirstWriteCell(0x200),
                SITestUtils.getMockFirstWriteCell(0x100)
        ));
        TxnView transaction1 = TxnTestUtils.getMockCommittedTxn(0x100, 0x110);
        TxnView transaction2 = TxnTestUtils.getMockCommittedTxn(0x200, 0x210);
        TxnView transaction3 = TxnTestUtils.getMockCommittedTxn(0x300, 0x310);
        TxnView transaction4 = TxnTestUtils.getMockCommittedTxn(0x400, 0x410);
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
        cutPurgeDuringMinorCompaction.mutate(inputCells, transactions, outputCells);
        assertThat(outputCells, contains(
                SITestUtils.getMockCommitCell(0x400, 0x410),
                SITestUtils.getMockTombstoneCell(0x400)
        ));
    }

    @Test
    public void mutateDoNotPurgeLatestTombstoneDuringMinorCompactionMissingFirstWriteToken() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(0x200, 0x210),
                SITestUtils.getMockCommitCell(0x100, 0x110),
                SITestUtils.getMockTombstoneCell(0x200),
                SITestUtils.getMockValueCell(0x100),
                SITestUtils.getMockDeleteRightAfterFirstWriteCell(0x200)
        ));
        TxnView transaction1 = TxnTestUtils.getMockCommittedTxn(0x100, 0x110);
        TxnView transaction2 = TxnTestUtils.getMockCommittedTxn(0x200, 0x210);
        transactions.addAll(Arrays.asList(
                null,
                null,
                transaction2,
                transaction1,
                transaction2
        ));
        cutPurgeDuringMinorCompaction.mutate(inputCells, transactions, outputCells);
        assertThat(outputCells, hasSize(3));
    }

    @Test
    public void mutateDoNotPurgeLatestTombstoneDuringMinorCompactionMissingDeleteToken() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(0x200, 0x210),
                SITestUtils.getMockCommitCell(0x100, 0x110),
                SITestUtils.getMockTombstoneCell(0x200),
                SITestUtils.getMockValueCell(0x100),
                SITestUtils.getMockFirstWriteCell(0x100)
        ));
        TxnView transaction1 = TxnTestUtils.getMockCommittedTxn(0x100, 0x110);
        TxnView transaction2 = TxnTestUtils.getMockCommittedTxn(0x200, 0x210);
        transactions.addAll(Arrays.asList(
                null,
                null,
                transaction2,
                transaction1,
                transaction1
        ));
        cutPurgeDuringMinorCompaction.mutate(inputCells, transactions, outputCells);
        assertThat(outputCells, hasSize(2));
    }

    @Test
    public void mutatePurgeDuringMinorCompactionAntiTombstoneGhostsTombstone() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(0x200, 0x210),
                SITestUtils.getMockCommitCell(0x100, 0x110),
                SITestUtils.getMockAntiTombstoneCell(0x200),
                SITestUtils.getMockValueCell(0x200, new boolean[]{true, false, false}),
                SITestUtils.getMockValueCell(0x100, new boolean[]{false, true, false}),
                SITestUtils.getMockDeleteRightAfterFirstWriteCell(0x200),
                SITestUtils.getMockFirstWriteCell(0x100)
        ));
        TxnView transaction1 = TxnTestUtils.getMockCommittedTxn(0x100, 0x110);
        TxnView transaction2 = TxnTestUtils.getMockCommittedTxn(0x200, 0x210);
        transactions.addAll(Arrays.asList(
                null,
                null,
                transaction2,
                transaction2,
                transaction1,
                transaction2,
                transaction1
        ));
        cutPurgeDuringMinorCompaction.mutate(inputCells, transactions, outputCells);
        assertThat(outputCells, equalTo(inputCells));
    }

    @Test
    public void mutateCannotPurgeNonGhostingUpdates() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(0x300, 0x310),
                SITestUtils.getMockCommitCell(0x200, 0x210),
                SITestUtils.getMockCommitCell(0x100, 0x110),
                SITestUtils.getMockValueCell(0x300, new boolean[]{false, true, false}),
                SITestUtils.getMockValueCell(0x200, new boolean[]{true, false, false}),
                SITestUtils.getMockValueCell(0x100, new boolean[]{true, true, true})
        ));
        transactions.addAll(Arrays.asList(
                null,
                null,
                null,
                TxnTestUtils.getMockCommittedTxn(0x300, 0x310),
                TxnTestUtils.getMockCommittedTxn(0x200, 0x210),
                TxnTestUtils.getMockCommittedTxn(0x100, 0x110)
        ));
        cutForcePurge.mutate(inputCells, transactions, outputCells);
        assertThat(outputCells, equalTo(inputCells));
    }

    @Test
    public void mutatePurgeOneColumnUpdatedOverAndOver() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(0x300, 0x310),
                SITestUtils.getMockCommitCell(0x200, 0x210),
                SITestUtils.getMockCommitCell(0x100, 0x110),
                SITestUtils.getMockValueCell(0x300, new boolean[]{true}),
                SITestUtils.getMockValueCell(0x200, new boolean[]{true}),
                SITestUtils.getMockValueCell(0x100, new boolean[]{true})
        ));
        transactions.addAll(Arrays.asList(
                null,
                null,
                null,
                TxnTestUtils.getMockCommittedTxn(0x300, 0x310),
                TxnTestUtils.getMockCommittedTxn(0x200, 0x210),
                TxnTestUtils.getMockCommittedTxn(0x100, 0x110)
        ));
        cutForcePurge.mutate(inputCells, transactions, outputCells);
        assertThat(outputCells, hasSize(2));
        assertThat(outputCells, contains(
                SITestUtils.getMockCommitCell(0x300, 0x310),
                SITestUtils.getMockValueCell(0x300, new boolean[]{true})
        ));
    }

    @Test
    public void mutatePurgePartialUpdateButNotBaselineInsert() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(0x300, 0x310),
                SITestUtils.getMockCommitCell(0x200, 0x210),
                SITestUtils.getMockCommitCell(0x100, 0x110),
                SITestUtils.getMockValueCell(0x300, new boolean[]{true, false}),
                SITestUtils.getMockValueCell(0x200, new boolean[]{true, false}),
                SITestUtils.getMockValueCell(0x100, new boolean[]{true, true})
        ));
        transactions.addAll(Arrays.asList(
                null,
                null,
                null,
                TxnTestUtils.getMockCommittedTxn(0x300, 0x310),
                TxnTestUtils.getMockCommittedTxn(0x200, 0x210),
                TxnTestUtils.getMockCommittedTxn(0x100, 0x110)
        ));
        cutForcePurge.mutate(inputCells, transactions, outputCells);
        assertThat(outputCells, hasSize(4));
        assertThat(getRemovedCells(inputCells, outputCells), containsInAnyOrder(
                SITestUtils.getMockValueCell(0x200, new boolean[]{true, false}),
                SITestUtils.getMockCommitCell(0x200, 0x210)
        ));
    }

    @Test
    public void mutatePurgeBaselineUpdateBecausePartialUpdatesCoverItAll() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(0x300, 0x310),
                SITestUtils.getMockCommitCell(0x200, 0x210),
                SITestUtils.getMockCommitCell(0x100, 0x110),
                SITestUtils.getMockValueCell(0x300, new boolean[]{false, true}),
                SITestUtils.getMockValueCell(0x200, new boolean[]{true, false}),
                SITestUtils.getMockValueCell(0x100, new boolean[]{true, true})
        ));
        transactions.addAll(Arrays.asList(
                null,
                null,
                null,
                TxnTestUtils.getMockCommittedTxn(0x300, 0x310),
                TxnTestUtils.getMockCommittedTxn(0x200, 0x210),
                TxnTestUtils.getMockCommittedTxn(0x100, 0x110)
        ));
        cutForcePurge.mutate(inputCells, transactions, outputCells);
        assertThat(outputCells, hasSize(4));
        assertThat(getRemovedCells(inputCells, outputCells), containsInAnyOrder(
                SITestUtils.getMockValueCell(0x100, new boolean[]{true, true}),
                SITestUtils.getMockCommitCell(0x100, 0x110)
        ));
    }

    @Test
    public void mutateDoNotPurgeUpdateBecauseOverrideIsAboveWatermark() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(0x1200, 0x1210),
                SITestUtils.getMockCommitCell(0x100, 0x110),
                SITestUtils.getMockValueCell(0x1200, new boolean[]{true}),
                SITestUtils.getMockValueCell(0x100, new boolean[]{true})
        ));
        transactions.addAll(Arrays.asList(
                null,
                null,
                TxnTestUtils.getMockCommittedTxn(0x1200, 0x1210),
                TxnTestUtils.getMockCommittedTxn(0x100, 0x110)
        ));
        cutForcePurge.mutate(inputCells, transactions, outputCells);
        assertThat(outputCells, equalTo(inputCells));
    }

    @Test
    public void mutateDoNotPurgeOldUpdateBecauseOverrideStartedBeforeWatermarkButCommittedAfter() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(0x900, 0x1100),
                SITestUtils.getMockCommitCell(0x100, 0x110),
                SITestUtils.getMockValueCell(0x900),
                SITestUtils.getMockValueCell(0x100)
        ));
        transactions.addAll(Arrays.asList(
                null,
                null,
                TxnTestUtils.getMockCommittedTxn(0x900, 0x1100),
                TxnTestUtils.getMockCommittedTxn(0x100, 0x110)
        ));
        cutForcePurge.mutate(inputCells, transactions, outputCells);
        assertThat(outputCells, equalTo(inputCells));
    }

    @Test
    public void isCommitted() {
        TxnView committedWithRootParent = TxnTestUtils.getMockCommittedTxn(0x100, 0x200);
        assertTrue(SICompactionStateMutate.isCommitted(committedWithRootParent));

        TxnView committedWithCommittedParent = TxnTestUtils.getMockCommittedTxn(0x100, 0x200, committedWithRootParent);
        assertTrue(SICompactionStateMutate.isCommitted(committedWithCommittedParent));

        TxnView committedThirdGeneration = TxnTestUtils.getMockCommittedTxn(0x100, 0x200, committedWithCommittedParent);
        assertTrue(SICompactionStateMutate.isCommitted(committedThirdGeneration));

        TxnView activeWithRootParent = TxnTestUtils.getMockActiveTxn(0x100);
        assertFalse(SICompactionStateMutate.isCommitted(activeWithRootParent));

        TxnView activeWithCommittedParent = TxnTestUtils.getMockActiveTxn(0x150, committedWithCommittedParent);
        assertFalse(SICompactionStateMutate.isCommitted(activeWithCommittedParent));

        TxnView activeWithActiveParent = TxnTestUtils.getMockActiveTxn(0x150, activeWithRootParent);
        assertFalse(SICompactionStateMutate.isCommitted(activeWithActiveParent));

        TxnView committedWithActiveParent = TxnTestUtils.getMockCommittedTxn(0x150, 0x250, activeWithRootParent);
        assertFalse(SICompactionStateMutate.isCommitted(committedWithActiveParent));
    }
}