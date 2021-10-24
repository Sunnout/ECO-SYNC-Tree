package protocols.replication.requests;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;

public class PrintStateRequest extends ProtoRequest {

    public static final short REQUEST_ID = 610;

    public PrintStateRequest() {
        super(REQUEST_ID);
    }
}
