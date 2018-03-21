package com.splicemachine.derby.stream.iapi;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;

import java.util.List;

/**
 * Created by jyuan on 2/12/18.
 */
public interface TableChecker {
    List<String> checkIndex(PairDataSet index,
                            String indexName,
                            KeyHashDecoder indexKeyDecoder,
                            ExecRow indexKey) throws Exception;
}
