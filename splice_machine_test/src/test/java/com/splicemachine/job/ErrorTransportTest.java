package com.splicemachine.job;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.junit.Assert.*;

public class ErrorTransportTest {

    @Test
    public void standardException() throws Exception {
        // given
        StandardException origStdException = StandardException.closeException();
        ErrorTransport origTransport = ErrorTransport.newTransport(origStdException);
        assertEquals(StandardException.class, origTransport.getError().getClass());
        assertFalse(origTransport.shouldRetry());

        // when
        ErrorTransport transportRoundTrip = serializeAndDeserialize(origTransport);

        // then
        StandardException stdExceptionRoundTrip = transportRoundTrip.getStandardException();
        assertEquals(StandardException.class, stdExceptionRoundTrip.getClass());
        assertEquals(origStdException.getMessage(), stdExceptionRoundTrip.getMessage());
        assertEquals(origStdException.getMessageId(), stdExceptionRoundTrip.getMessageId());
        assertFalse(transportRoundTrip.shouldRetry());
    }

    @Test
    public void standardException_withArgs() throws Exception {
        // given
        StandardException origStdException = StandardException.newException(SQLState.CLOSE_REQUEST, "testArg1", "testArg2", "testArg3");
        ErrorTransport origTransport = ErrorTransport.newTransport(origStdException);
        assertEquals(StandardException.class, origTransport.getError().getClass());
        assertFalse(origTransport.shouldRetry());

        // when
        ErrorTransport transportRoundTrip = serializeAndDeserialize(origTransport);

        // then
        StandardException stdExceptionRoundTrip = transportRoundTrip.getStandardException();
        assertEquals(StandardException.class, stdExceptionRoundTrip.getClass());
        assertEquals(origStdException.getMessage(), stdExceptionRoundTrip.getMessage());
        assertEquals(origStdException.getMessageId(), stdExceptionRoundTrip.getMessageId());
        assertArrayEquals(new Object[]{"testArg1", "testArg2", "testArg3"}, stdExceptionRoundTrip.getArguments());
        assertFalse(transportRoundTrip.shouldRetry());
    }

    @Test
    public void arbitraryThrowable() throws Exception {
        // given
        Exception origException = new IllegalStateException("testMessage");
        ErrorTransport origTransport = ErrorTransport.newTransport(origException);
        assertEquals(IllegalStateException.class, origTransport.getError().getClass());
        assertTrue(origTransport.shouldRetry());

        // when
        ErrorTransport transportRoundTrip = serializeAndDeserialize(origTransport);

        // then
        IllegalStateException exceptionRoundTrip = (IllegalStateException) transportRoundTrip.getError();
        assertEquals(origException.getMessage(), exceptionRoundTrip.getMessage());
        assertTrue(transportRoundTrip.shouldRetry());
    }

    @Test
    public void arbitraryThrowable_nullMessage() throws Exception {
        // given
        Exception origException = new IllegalStateException();
        ErrorTransport origTransport = ErrorTransport.newTransport(origException);
        assertEquals(IllegalStateException.class, origTransport.getError().getClass());
        assertTrue(origTransport.shouldRetry());
        assertNull(origException.getMessage());

        // when
        ErrorTransport transportRoundTrip = serializeAndDeserialize(origTransport);

        // then
        IllegalStateException exceptionRoundTrip = (IllegalStateException) transportRoundTrip.getError();
        assertEquals("this is what we do", "null", exceptionRoundTrip.getMessage());
        assertTrue(transportRoundTrip.shouldRetry());
    }

    @Test
    public void arbitraryThrowable_withoutOneArgStringConstructor() throws Exception {
        // given
        Exception origException = new NoOneArgConstructor();
        ErrorTransport origTransport = ErrorTransport.newTransport(origException);
        assertEquals(NoOneArgConstructor.class, origTransport.getError().getClass());
        assertTrue(origTransport.shouldRetry());

        // when
        ErrorTransport transportRoundTrip = serializeAndDeserialize(origTransport);

        // then
        NoOneArgConstructor exceptionRoundTrip = (NoOneArgConstructor) transportRoundTrip.getError();
        assertEquals(origException.getMessage(), exceptionRoundTrip.getMessage());
        assertTrue(transportRoundTrip.shouldRetry());
    }

    @Test
    @Ignore("Ignored until I figure out what this does")
    public void arbitraryThrowable_withoutNoPublicConstructor() throws Exception {
        // given
        Exception origException = new NoPublicConstructor();
        ErrorTransport origTransport = ErrorTransport.newTransport(origException);
        try {
            origTransport.getError().getClass();
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("Could not find valid constructor for class = com.splicemachine.job.ErrorTransportTest$NoPublicConstructor", e.getMessage());
        }
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    private ErrorTransport serializeAndDeserialize(ErrorTransport errorTransport) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(errorTransport);
        oos.close();

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        return (ErrorTransport) ois.readObject();
    }

    public static class NoOneArgConstructor extends RuntimeException {
    }

    public static class NoPublicConstructor extends RuntimeException {
        private NoPublicConstructor() {
        }
    }

}