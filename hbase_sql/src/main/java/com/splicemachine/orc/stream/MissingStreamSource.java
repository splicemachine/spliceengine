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
package com.splicemachine.orc.stream;

import javax.annotation.Nullable;
import java.io.IOException;

public class MissingStreamSource<S extends ValueStream<?>> implements StreamSource<S>
{
    private final Class<S> streamType;

    public static <S extends ValueStream<?>> StreamSource<S> missingStreamSource(Class<S> streamType)
    {
        return new MissingStreamSource<>(streamType);
    }

    private MissingStreamSource(Class<S> streamType)
    {
        this.streamType = streamType;
    }

    @Override
    public Class<S> getStreamType()
    {
        return streamType;
    }

    @Nullable
    @Override
    public S openStream()
            throws IOException
    {
        return null;
    }
}
