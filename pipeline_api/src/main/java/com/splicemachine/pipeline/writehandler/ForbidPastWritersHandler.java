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

package com.splicemachine.pipeline.writehandler;

import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.si.impl.DDLFilter;
import org.apache.log4j.Logger;
import java.io.IOException;

public class ForbidPastWritersHandler implements WriteHandler {
    private static final Logger LOG = Logger.getLogger(ForbidPastWritersHandler.class);

    private DDLFilter ddlFilter;

    public ForbidPastWritersHandler(DDLFilter ddlFilter) {
        super();
        this.ddlFilter = ddlFilter;
    }

    @Override
    public void next(KVPair mutation, WriteContext ctx) {
        try {
            if (!ddlFilter.isVisibleBy(ctx.getTxn())) {
                ctx.failed(mutation, WriteResult.failed("Writes forbidden by transaction " + ddlFilter.getTransaction()));
            }else ctx.sendUpstream(mutation);
        } catch (IOException e) {
            LOG.error("Couldn't asses the visibility of the DDL operation", e);
            ctx.failed(mutation, WriteResult.failed(e.getMessage()));
        }
    }

    @Override
    public void flush(WriteContext ctx) throws IOException {
        // no op
    }

	@Override
	public void close(WriteContext ctx) throws IOException {
		// no op
	}


}
