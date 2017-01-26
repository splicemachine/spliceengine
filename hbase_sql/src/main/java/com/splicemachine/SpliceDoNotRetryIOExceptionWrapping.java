/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import com.splicemachine.db.iapi.error.StandardException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility methods for wrapping a StandardException in a IOException.
 */
public class SpliceDoNotRetryIOExceptionWrapping {

    private static final Gson GSON = new GsonBuilder()
            .registerTypeAdapter(StandardException.class, new StandardExceptionAdapter())
            .create();

    private static final String OPEN_BRACE = "{";
    private static final String CLOSE_BRACE = "}";
    private static final String COLON = ":";

    /**
     * UNWRAP from SpliceDoNotRetryIOException
     */
    public static Exception unwrap(SpliceDoNotRetryIOException spliceException) {
        String fullMessage = spliceException.getMessage();
        int firstColonIndex = fullMessage.indexOf(COLON);
        int openBraceIndex = fullMessage.indexOf(OPEN_BRACE);
        String exceptionType;
        if (firstColonIndex < openBraceIndex) {
            exceptionType = fullMessage.substring(firstColonIndex + 1, openBraceIndex).trim();
        } else {
            exceptionType = fullMessage.substring(0, openBraceIndex).trim();
        }
        Class exceptionClass;
        try {
            exceptionClass = Class.forName(exceptionType);
        } catch (ClassNotFoundException e1) {
            // shouldn't happen, but if it does, we'll just use Exception.class and deal with the fact that it might not be 100% correct
            exceptionClass = Exception.class;
        }
        String json = fullMessage.substring(openBraceIndex, fullMessage.indexOf(CLOSE_BRACE) + 1);
        return (Exception) GSON.fromJson(json, exceptionClass);
    }

    /**
     * WRAP in SpliceDoNotRetryIOException
     */
    public static SpliceDoNotRetryIOException wrap(StandardException se) {
        String namePlusJson = se.getClass().getCanonicalName() + GSON.toJson(se);
        return new SpliceDoNotRetryIOException(namePlusJson);
    }

    private static class StandardExceptionAdapter extends TypeAdapter<StandardException> {

        @Override
        public void write(JsonWriter out, StandardException value) throws IOException {
            out.beginObject();

            out.name("severity").value(value.getErrorCode());
            out.name("textMessage").value(value.getTextMessage());
            out.name("sqlState").value(value.getSqlState());
            out.name("messageId").value(value.getMessageId());

            out.name("arguments");
            Object[] args = value.getArguments();
            if (args == null)
                out.nullValue();
            else {
                out.beginArray();
                for (Object o : value.getArguments()) {
                    if (o instanceof String) {
                        out.value((String) o);
                    }
                }
                out.endArray();
            }
            out.endObject();
        }

        @Override
        public StandardException read(JsonReader in) throws IOException {
            in.beginObject();

            int severity = 0;
            String textMessage = null;
            String sqlState = null;
            String messageId = null;
            List<String> objectStrings = null;
            while (in.peek() != JsonToken.END_OBJECT) {
                String nextName = in.nextName();
                if ("severity".equals(nextName))
                    severity = in.nextInt();
                else if ("textMessage".equals(nextName))
                    textMessage = in.nextString();
                else if ("sqlState".equals(nextName))
                    sqlState = in.nextString();
                else if ("messageId".equals(nextName))
                    messageId = in.nextString();
                else if ("arguments".equals(nextName)) {
                    if (in.peek() != JsonToken.NULL) {
                        in.beginArray();
                        objectStrings = new ArrayList<String>();
                        while (in.peek() != JsonToken.END_ARRAY) {
                            objectStrings.add(in.nextString());
                        }
                        in.endArray();
                    } else
                        in.nextNull();
                }
            }

            in.endObject();

            StandardException se;
            if (objectStrings != null) {
                Object[] objects = new Object[objectStrings.size()];
                objectStrings.toArray(objects);
                se = StandardException.newException(messageId, objects);
            } else {
                se = StandardException.newException(messageId);
            }
            se.setSeverity(severity);
            se.setSqlState(sqlState);
            se.setTextMessage(textMessage);

            return se;
        }
    }

}
