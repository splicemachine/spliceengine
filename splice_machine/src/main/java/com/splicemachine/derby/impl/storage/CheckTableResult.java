/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.storage;

import com.splicemachine.derby.iapi.sql.olap.AbstractOlapResult;

import java.util.List;
import java.util.Map;

/**
 * Created by jyuan on 2/7/18.
 */
public class CheckTableResult extends AbstractOlapResult {

    private Map<String, List<String>> results;
    private boolean isSuccess = true;

    public CheckTableResult() {
    }

    public CheckTableResult(Map<String, List<String>> results) {
        this.results = results;
    }

    public void setSuccess(boolean isSuccess) {
        this.isSuccess = isSuccess;
    }

    @Override
    public boolean isSuccess(){
        return isSuccess;
    }

    public void setResults(Map<String, List<String>> results) {
        this.results = results;
    }

    public Map<String, List<String>> getResults() {
        return results;
    }
}
