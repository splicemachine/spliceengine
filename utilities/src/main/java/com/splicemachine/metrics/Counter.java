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

package com.splicemachine.metrics;

/**
 * Represents a basic counter.
 *
 * We could of course just use longs directly, but then we'd have to
 * do boolean checks every time to determine if we should collect. This sucks,
 * when we could just do that boolean check once and use polymorphism to do
 * No-ops when we don't need to count.
 *
 * @author Scott Fines
 * Date: 1/17/14
 */
public interface Counter {

		void increment();

		void add(long value);

		long getTotal();

		boolean isActive();
}
