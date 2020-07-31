package com.splicemachine.ck.visitor;

import com.splicemachine.ck.decoder.UserDataDecoder;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;

import java.util.ArrayList;
import java.util.List;

public class TableRowPrinter implements IRowPrinter {

    private final TableCellPrinter tableCellPrinter;

    public TableRowPrinter(UserDataDecoder decoder) {
        this.tableCellPrinter = new TableCellPrinter(decoder);
    }

    @Override
    public List<String> ProcessRow(Result row) throws Exception {
        List<String> result = new ArrayList<>();
        for(Cell cell : row.listCells()) {
            tableCellPrinter.visit(cell);
        }
        result.add(tableCellPrinter.getOutput());
        return result;
    }
}
