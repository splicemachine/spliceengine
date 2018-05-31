package com.splicemachine.io.hbase.encoding;

import com.splicemachine.si.constants.SIConstants;

/**
 *
 * PAX Redo Cell implementation...
 *
 */
public class PAXRedoCell extends PAXActiveCell  {

    public PAXRedoCell(int seekerPosition, byte[] extendedRowKey, LazyColumnarSeeker lazyColumnarSeeker, long timestamp) {
        super(seekerPosition,extendedRowKey,lazyColumnarSeeker,timestamp);
    }

    @Override
    public byte[] getFamilyArray() {
        return SIConstants.DEFAULT_FAMILY_REDO_BYTES;
    }

    @Override
    public byte[] getFamily() {
        return SIConstants.DEFAULT_FAMILY_REDO_BYTES;
    }

}
