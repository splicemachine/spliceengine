package com.splicemachine.job;

import com.splicemachine.async.RegionMovedException;
import com.splicemachine.pipeline.constraint.ConstraintContext;
import com.splicemachine.pipeline.constraint.ConstraintViolation;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.shared.common.reference.SQLState;
import org.junit.*;
import org.junit.Ignore;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.junit.Assert.*;

public class ErrorTransportTest {


    @Test
    public void testDB3874() throws Exception {
        String className = "com.splicemachine.async.RegionMovedException";
            Class<? extends Throwable> asyncClazz = (Class<? extends Throwable>) Class.forName(className);
            Assert.assertTrue(com.splicemachine.async.RecoverableException.class.isAssignableFrom(asyncClazz));
    }

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
    public void constraintViolation() throws Exception {
        // given
        ConstraintContext constraintContext = new ConstraintContext("a", "b", "c", "d");
        Exception origException = new ConstraintViolation.ForeignKeyConstraintViolation(constraintContext);
        ErrorTransport origTransport = ErrorTransport.newTransport(origException);
        assertEquals(StandardException.class, origTransport.getError().getClass());
        assertFalse(origTransport.shouldRetry());

        // when
        ErrorTransport transportRoundTrip = serializeAndDeserialize(origTransport);

        // then
        StandardException exceptionRoundTrip = (StandardException) transportRoundTrip.getError();
        assertEquals("c on table 'b' caused a violation of foreign key constraint 'a' for key d.  The statement has been rolled back.", exceptionRoundTrip.getMessage());
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
    public void arbitraryThrowable_withoutPublicConstructor() throws Exception {
        // given
        Exception origException = new NoPublicConstructor();
        ErrorTransport origTransport = ErrorTransport.newTransport(origException);
        try {
            origTransport.getError().getClass();
            fail();
        } catch (RuntimeException e) {
            assertEquals("java.lang.IllegalAccessException: Class com.splicemachine.job.ErrorTransport can not access a member of class com.splicemachine.job.ErrorTransportTest$NoPublicConstructor with modifiers \"private\"", e.getMessage());
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