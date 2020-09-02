package com.splicemachine.si.impl.server;

import com.splicemachine.hbase.CellUtils;
import com.splicemachine.si.api.txn.Txn;
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
    private long watermark = 1000;
    private SICompactionStateMutate cutNoPurge = new SICompactionStateMutate(
            new PurgeConfigBuilder().noPurge().transactionLowWatermark(watermark).build());
    private SICompactionStateMutate cutForcePurge = new SICompactionStateMutate(
            new PurgeConfigBuilder().forcePurgeDeletes().purgeUpdates(true).transactionLowWatermark(watermark).build());
    private SICompactionStateMutate cutPurgeDuringFlush = new SICompactionStateMutate(
            new PurgeConfigBuilder().purgeDeletesDuringFlush().purgeUpdates(true).transactionLowWatermark(watermark).build());
    private SICompactionStateMutate cutPurgeDuringMajorCompaction = new SICompactionStateMutate(
            new PurgeConfigBuilder().purgeDeletesDuringMajorCompaction().purgeUpdates(true).transactionLowWatermark(watermark).build());
    private SICompactionStateMutate cutPurgeDuringMinorCompaction = new SICompactionStateMutate(
            new PurgeConfigBuilder().purgeDeletesDuringMinorCompaction().purgeUpdates(true).transactionLowWatermark(watermark).build());
    private List<Cell> inputCells = new ArrayList<>();
    private List<TxnView> transactions = new ArrayList<>();
    private List<Cell> outputCells = new ArrayList<>();

    public SICompactionStateMutateTest() throws IOException {
    }

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
                SITestUtils.getMockValueCell(200),
                SITestUtils.getMockValueCell(100)));
        transactions.addAll(Arrays.asList(
                TxnTestUtils.getMockActiveTxn(200),
                TxnTestUtils.getMockActiveTxn(100)
        ));
        cutNoPurge.mutate(inputCells, transactions, outputCells);
        assertThat(outputCells, equalTo(inputCells));
    }

    @Test
    public void mutateSimpleOneCommittedTransaction() throws IOException {
        inputCells.addAll(Collections.singletonList(
                SITestUtils.getMockValueCell(100)));
        transactions.addAll(Collections.singletonList(
                TxnTestUtils.getMockCommittedTxn(100, 200)));
        cutNoPurge.mutate(inputCells, transactions, outputCells);
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
        cutNoPurge.mutate(inputCells, transactions, outputCells);
        assertThat(outputCells, is(empty()));
    }

    @Test
    public void mutateMixOfTransactions() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockValueCell(300),
                SITestUtils.getMockValueCell(200),
                SITestUtils.getMockValueCell(100)
        ));
        transactions.addAll(Arrays.asList(
                TxnTestUtils.getMockCommittedTxn(300, 310),
                TxnTestUtils.getMockActiveTxn(200),
                TxnTestUtils.getMockRolledBackTxn(100)
        ));
        cutNoPurge.mutate(inputCells, transactions, outputCells);
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
        cutNoPurge.mutate(inputCells, transactions, outputCells);
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
        cutNoPurge.mutate(inputCells, transactions, outputCells);
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
        cutForcePurge.mutate(inputCells, transactions, outputCells);
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
        cutForcePurge.mutate(inputCells, transactions, outputCells);
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
        cutForcePurge.mutate(inputCells, transactions, outputCells);
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
        cutForcePurge.mutate(inputCells, transactions, outputCells);
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
        cutPurgeDuringFlush.mutate(inputCells, transactions, outputCells);
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
        cutPurgeDuringMajorCompaction.mutate(inputCells, transactions, outputCells);
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

        cutPurgeDuringMajorCompaction.mutate(inputCells, transactions, outputCells);
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

        cutPurgeDuringFlush.mutate(inputCells, transactions, outputCells);
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
        cutPurgeDuringMajorCompaction.mutate(inputCells, transactions, outputCells);
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
        cutPurgeDuringFlush.mutate(inputCells, transactions, outputCells);
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
        cutPurgeDuringMinorCompaction.mutate(inputCells, transactions, outputCells);
        assertThat(outputCells, is(empty()));
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
        cutPurgeDuringMinorCompaction.mutate(inputCells, transactions, outputCells);
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
        cutPurgeDuringMinorCompaction.mutate(inputCells, transactions, outputCells);
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
        TxnView transaction1 = TxnTestUtils.getMockCommittedTxn(100, 110);
        TxnView transaction2 = TxnTestUtils.getMockCommittedTxn(200, 210);
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
        cutPurgeDuringMinorCompaction.mutate(inputCells, transactions, outputCells);
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
        TxnView transaction = TxnTestUtils.getMockCommittedTxn(50, 400);
        transactions.addAll(Arrays.asList(
                transaction,
                transaction,
                transaction,
                transaction
        ));
        cutForcePurge.mutate(inputCells, transactions, outputCells);
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
        cutForcePurge.mutate(inputCells, transactions, outputCells);
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
        cutForcePurge.mutate(inputCells, transactions, outputCells);
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
        cutForcePurge.mutate(inputCells, transactions, outputCells);
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
        cutForcePurge.mutate(inputCells, transactions, outputCells);
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
        cutForcePurge.mutate(inputCells, transactions, outputCells);
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
        cutForcePurge.mutate(inputCells, transactions, outputCells);
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
        cutForcePurge.mutate(inputCells, transactions, outputCells);
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
        cutForcePurge.mutate(inputCells, transactions, outputCells);
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
        cutForcePurge.mutate(inputCells, transactions, outputCells);
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
        long actualSize = cutForcePurge.mutate(inputCells, transactions, outputCells);
        assertThat(actualSize, equalTo(SITestUtils.getSize(inputCells)));
    }
}
