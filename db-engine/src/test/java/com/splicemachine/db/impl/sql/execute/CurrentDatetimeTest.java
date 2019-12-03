/*
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

package com.splicemachine.db.impl.sql.execute;

import org.junit.Test;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * This class tests CurrentDatetime
 */
public class CurrentDatetimeTest {
    private CurrentDatetime cut = new CurrentDatetime();

    @Test
    public void testFixture() {
    }

    @Test
    public void testGetCurrentDate() {
        LocalDate local = LocalDate.now();
        Date date = cut.getCurrentDate();
        long difference = ChronoUnit.DAYS.between(local, date.toLocalDate());
        assertTrue(difference <= 1); // Mostly zero, unless ran around midnight
    }

    @Test
    public void testGetCurrentTime() {
        LocalTime local = LocalTime.now();
        Time time = cut.getCurrentTime();
        long difference = ChronoUnit.SECONDS.between(local, time.toLocalTime());
        assertTrue(difference <= 1);
    }

    @Test
    public void testGetCurrentTimestamp() {
        LocalDateTime local = LocalDateTime.now();
        Timestamp timestamp = cut.getCurrentTimestamp();
        long difference = ChronoUnit.MILLIS.between(local, timestamp.toLocalDateTime());
        assertTrue(difference <= 500);
    }

    @Test
    public void testForget() throws InterruptedException {
        Timestamp timestamp = cut.getCurrentTimestamp();
        TimeUnit.MILLISECONDS.sleep(10);
        Timestamp sameTimestamp = cut.getCurrentTimestamp();
        assertEquals(timestamp, sameTimestamp);

        TimeUnit.MILLISECONDS.sleep(10);
        cut.forget();
        Timestamp newTimestamp = cut.getCurrentTimestamp();
        assertNotEquals(timestamp, newTimestamp);
    }

    @Test
    public void testPrecision() {
        Timestamp timestamp = cut.getCurrentTimestamp();
        assertEquals(3, cut.getTimestampPrecision());

        for (int i = 0; i <= 9; ++i) {
            cut.setTimestampPrecision(i);
            assertEquals(i, cut.getTimestampPrecision());
            assertEquals(0, cut.getCurrentTimestamp().getNanos() % CurrentDatetime.POWERS_OF_10[9 - i]);
        }
    }
}
