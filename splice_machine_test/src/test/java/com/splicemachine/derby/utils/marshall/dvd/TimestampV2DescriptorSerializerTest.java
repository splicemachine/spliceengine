package com.splicemachine.derby.utils.marshall.dvd;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.shared.common.reference.SQLState;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.GregorianCalendar;

import static org.junit.Assert.assertEquals;


/**
 * Created by dmustafin on 5/13/15.
 */
public class TimestampV2DescriptorSerializerTest {

    @Test
    public void shouldSerialize() throws StandardException {
        long l = TimestampV2DescriptorSerializer.formatLong(getTimestamp(2000));
        assertEquals(950162400000000000L, l);

        l = TimestampV2DescriptorSerializer.formatLong(getTimestamp(1678));
        assertEquals(-9211082400000000000L, l);


        l = TimestampV2DescriptorSerializer.formatLong(getTimestamp(2262));
        assertEquals(+9218124000000000000L, l);
    }


    @Test
    public void shouldFailOnSmallDate() {
        String errorCode = "N/A";
        try {
            TimestampV2DescriptorSerializer.formatLong(getTimestamp(1677));
        } catch (StandardException e) {
            errorCode = e.getSqlState();
        }

        assertEquals(SQLState.LANG_DATE_TIME_ARITHMETIC_OVERFLOW, errorCode);
    }


    @Test
    public void shouldFailOnLargeDate() {
        String errorCode = "N/A";
        try {
            TimestampV2DescriptorSerializer.formatLong(getTimestamp(2263));
        } catch (StandardException e) {
            errorCode = e.getSqlState();
        }

        assertEquals(SQLState.LANG_DATE_TIME_ARITHMETIC_OVERFLOW, errorCode);
    }


    private Timestamp getTimestamp(int year) {
        GregorianCalendar cal = new GregorianCalendar(year, 01, 10);

        return new Timestamp(cal.getTimeInMillis());
    }

}
