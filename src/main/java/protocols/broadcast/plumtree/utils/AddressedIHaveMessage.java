package protocols.broadcast.plumtree.utils;

import pt.unl.fct.di.novasys.network.data.Host;
import protocols.broadcast.plumtree.messages.IHaveMessage;

public class AddressedIHaveMessage {
    public IHaveMessage msg;
    public Host to;

    public AddressedIHaveMessage(IHaveMessage msg, Host to) {
        this.msg = msg;
        this.to = to;
    }
}
