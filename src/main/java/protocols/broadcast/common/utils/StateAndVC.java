package protocols.broadcast.common.utils;

import io.netty.buffer.ByteBuf;
import serializers.MySerializer;

import java.io.IOException;


public class StateAndVC {

    private byte[] state;
    private VectorClock vc;

    public StateAndVC(byte[] state, VectorClock vc) {
        this.state = state;
        this.vc = vc;
    }

    public byte[] getState() {
        return state;
    }

    public VectorClock getVc() {
        return vc;
    }

    public void setState(byte[] state) {
        this.state = state;
    }

    public void setVC(VectorClock vc) {
        this.vc = vc;
    }

    public static MySerializer<StateAndVC> serializer = new MySerializer<StateAndVC>() {
        @Override
        public void serialize(StateAndVC vc, ByteBuf out) throws IOException {
            out.writeInt(vc.state.length);
            out.writeBytes(vc.state);
            VectorClock.serializer.serialize(vc.vc, out);
        }

        @Override
        public StateAndVC deserialize(ByteBuf in) throws IOException {
            int size = in.readInt();
            byte[] state = new byte[size];
            in.readBytes(state);
            VectorClock vc = VectorClock.serializer.deserialize(in);
            return new StateAndVC(state, vc);
        }
    };

    @Override
    public String toString() {
        return "StateAndVC{" +
                "vc=" + vc +
                '}';
    }

}
