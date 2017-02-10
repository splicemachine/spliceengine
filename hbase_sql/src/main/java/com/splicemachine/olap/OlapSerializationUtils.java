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

package com.splicemachine.olap;

import com.google.protobuf.ByteString;
import com.google.protobuf.ZeroCopyLiteralByteString;
import com.splicemachine.olap.OlapMessage;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;

import java.io.*;

/**
 * @author Scott Fines
 *         Date: 4/4/16
 */
class OlapSerializationUtils{

    @SuppressWarnings("unchecked")
    public static <R extends Serializable> R decode(ByteString commandBytes) throws IOException{
        InputStream is = commandBytes.newInput();
        ObjectInputStream ois = new ObjectInputStream(is);
        try{
            return (R)ois.readObject(); //shouldn't be a problem with any IOExceptions
        }catch(ClassNotFoundException e){
            throw new IOException(e); //shouldn't happen
        }
    }

    static ByteString encode(Serializable se) throws IOException{
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(se);
        oos.flush();
        oos.close();
        return ZeroCopyLiteralByteString.wrap(baos.toByteArray());
    }

    static OlapMessage.Response buildError(Throwable throwable) throws IOException{
        OlapMessage.Response.Builder response = OlapMessage.Response.newBuilder();

        OlapMessage.FailedResponse fr=OlapMessage.FailedResponse.newBuilder().setErrorBytes(encode(throwable)).build();
        response.setType(OlapMessage.Response.Type.FAILED);
        response.setExtension(OlapMessage.FailedResponse.response,fr);
        return response.build();
    }

    static OlapMessage.Response buildResponse(OlapStatus status,boolean[] shouldRemoveAfterWriting,long tickTime) throws IOException{
        OlapMessage.Response.Builder response = OlapMessage.Response.newBuilder();
        if(status==null){
            response.setType(OlapMessage.Response.Type.NOT_SUBMITTED);
            OlapMessage.ProgressResponse pr=OlapMessage.ProgressResponse.newBuilder().setTickTimeMillis(tickTime).build();
            response.setExtension(OlapMessage.ProgressResponse.response,pr);
        }else{
            switch(status.checkState()){
                case NOT_SUBMITTED:
                    response.setType(OlapMessage.Response.Type.NOT_SUBMITTED);
                    OlapMessage.ProgressResponse pr=OlapMessage.ProgressResponse.newBuilder().setTickTimeMillis(tickTime).build();
                    response.setExtension(OlapMessage.ProgressResponse.response,pr);
                case SUBMITTED:
                case RUNNING:
                    response.setType(OlapMessage.Response.Type.IN_PROGRESS);
                    OlapMessage.ProgressResponse build=OlapMessage.ProgressResponse.newBuilder().setTickTimeMillis(tickTime).build();
                    response.setExtension(OlapMessage.ProgressResponse.response,build);
                    break;
                case CANCELED:
                    shouldRemoveAfterWriting[0]=true;
                    OlapMessage.CancelledResponse cr=OlapMessage.CancelledResponse.getDefaultInstance();
                    response.setType(OlapMessage.Response.Type.CANCELLED);
                    response.setExtension(OlapMessage.CancelledResponse.response,cr);
                    break;
                case FAILED:
                    shouldRemoveAfterWriting[0]=true;
                    Throwable throwable=status.getResult().getThrowable();
                    OlapMessage.FailedResponse fr=OlapMessage.FailedResponse.newBuilder().setErrorBytes(encode(throwable)).build();
                    response.setType(OlapMessage.Response.Type.FAILED);
                    response.setExtension(OlapMessage.FailedResponse.response,fr);
                    break;
                case COMPLETE:
                    shouldRemoveAfterWriting[0]=true;
                    OlapMessage.Result r=OlapMessage.Result.newBuilder().setResultBytes(encode(status.getResult())).build();
                    response.setType(OlapMessage.Response.Type.COMPLETED);
                    response.setExtension(OlapMessage.Result.response,r);
                    break;
                default:
                    throw new IllegalStateException("Programmer error: unexpected state!");
            }
        }
        return response.build();
    }
}
