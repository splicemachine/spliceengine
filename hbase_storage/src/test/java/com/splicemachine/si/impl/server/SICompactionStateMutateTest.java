package com.splicemachine.si.impl.server;

import com.splicemachine.hbase.CellUtils;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.TxnTestUtils;
import com.splicemachine.storage.CellType;
import org.apache.hadoop.hbase.Cell;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.*;

public class SICompactionStateMutateTest {
    SICompactionStateMutate cut = new SICompactionStateMutate(false);
    SICompactionStateMutate cutPurge = new SICompactionStateMutate(true);
    List<Cell> inputCells = new ArrayList<>();
    List<TxnView> transactions = new ArrayList<>();
    List<Cell> outputCells = new ArrayList<>();

    public List<Cell> getNewlyAddedCells(List<Cell> inputCells, List<Cell> outputCells) {
        Set<Cell> set = new HashSet<>(outputCells);
        set.removeAll(new HashSet<>(inputCells));
        return new ArrayList<>(set);
    }

    public List<Cell> getRemovedCells(List<Cell> inputCells, List<Cell> outputCells) {
        return getNewlyAddedCells(outputCells, inputCells);
    }

    @Test
    public void mutateEmpty() throws IOException {
        cut.mutate(inputCells, transactions, outputCells);
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
        cut.mutate(inputCells, transactions, outputCells);
        assertThat(outputCells, equalTo(inputCells));
    }

    @Test
    public void mutateSimpleOneCommittedTransaction() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(0x200),
                SITestUtils.getMockValueCell(0x100)));
        transactions.addAll(Arrays.asList(
                null,
                TxnTestUtils.getMockCommittedTxn(0x100, 0x200)));
        cut.mutate(inputCells, transactions, outputCells);
        assertThat(outputCells, hasSize(inputCells.size() + 1));
        List<Cell> newCells = getNewlyAddedCells(inputCells, outputCells);
        assertThat(newCells, hasSize(1));
        assertEquals(CellType.COMMIT_TIMESTAMP, CellUtils.getKeyValueType(newCells.get(0)));
        assertEquals(0x100, newCells.get(0).getTimestamp());
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
        cut.mutate(inputCells, transactions, outputCells);
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
        cut.mutate(inputCells, transactions, outputCells);
        assertThat(outputCells, hasSize(inputCells.size())); // + 1 commit, -1 rolled back
        List<Cell> newCells = getNewlyAddedCells(inputCells, outputCells);
        List<Cell> removedCells = getRemovedCells(inputCells, outputCells);
        assertThat(newCells, hasSize(1));
        assertThat(removedCells, hasSize(1));
        assertEquals(0x100, removedCells.get(0).getTimestamp());
        assertEquals(0x300, newCells.get(0).getTimestamp());
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
        cut.mutate(inputCells, transactions, outputCells);
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
        cut.mutate(inputCells, transactions, outputCells);
        assertThat(getRemovedCells(inputCells, outputCells), is(empty()));
    }

    @Test
    public void mutatePurgeTombstones() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(0x200),
                SITestUtils.getMockCommitCell(0x100),
                SITestUtils.getMockTombstoneCell(0x200),
                SITestUtils.getMockValueCell(0x100)
        ));
        transactions.addAll(Arrays.asList(
                null,
                null,
                TxnTestUtils.getMockCommittedTxn(0x200, 0x210),
                TxnTestUtils.getMockCommittedTxn(0x100, 0x110)
        ));
        cutPurge.mutate(inputCells, transactions, outputCells);
        assertThat(outputCells, is(empty()));
    }

    @Test
    public void mutateDoNotPurgeBecauseTombstoneNotCommitted() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(0x100),
                SITestUtils.getMockTombstoneCell(0x200),
                SITestUtils.getMockValueCell(0x100)
        ));
        transactions.addAll(Arrays.asList(
                null,
                TxnTestUtils.getMockActiveTxn(0x200),
                TxnTestUtils.getMockCommittedTxn(0x100, 0x110)
        ));
        cutPurge.mutate(inputCells, transactions, outputCells);
        assertThat(outputCells, equalTo(inputCells));
    }

    @Test
    public void mutateDoNotPurgeBecauseTombstoneRolledBack() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(0x100),
                SITestUtils.getMockTombstoneCell(0x200),
                SITestUtils.getMockValueCell(0x100)
        ));
        transactions.addAll(Arrays.asList(
                null,
                TxnTestUtils.getMockRolledBackTxn(0x200),
                TxnTestUtils.getMockCommittedTxn(0x100, 0x110)
        ));
        cutPurge.mutate(inputCells, transactions, outputCells);
        assertThat(outputCells, hasSize(2));
        assertThat(outputCells, equalTo(Arrays.asList(
                SITestUtils.getMockCommitCell(0x100),
                SITestUtils.getMockValueCell(0x100))));
    }

    @Test
    public void mutatePartialPurge() throws IOException {
        inputCells.addAll(Arrays.asList(
                SITestUtils.getMockCommitCell(0x300),
                SITestUtils.getMockCommitCell(0x200),
                SITestUtils.getMockCommitCell(0x100),
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
        cutPurge.mutate(inputCells, transactions, outputCells);
        assertThat(outputCells, hasSize(3));
        assertThat(outputCells, equalTo(Arrays.asList(
                SITestUtils.getMockCommitCell(0x300),
                SITestUtils.getMockAntiTombstoneCell(0x300),
                SITestUtils.getMockValueCell(0x300))));
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