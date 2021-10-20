package protocols.replication;

import crdts.interfaces.SetCRDT;
import crdts.operations.Operation;
import crdts.operations.SetOperation;
import crdts.utils.TaggedElement;
import datatypes.SerializableType;
import io.netty.buffer.ByteBuf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.replication.requests.DownstreamRequest;
import pt.unl.fct.di.novasys.network.data.Host;
import serializers.MyCRDTSerializer;
import serializers.MySerializer;

import java.io.IOException;
import java.util.*;


public class ORSetCRDT implements SetCRDT, KernelCRDT {

    private static final Logger logger = LogManager.getLogger(ORSetCRDT.class);

    public enum SetOpType{
        ADD,
        REMOVE
    }

    private static final String CRDT_TYPE = "or_set";
    private static final String SET_ADD = "set_add";
    private static final String SET_REMOVE = "set_rem";

    private final CRDTCommunicationInterface kernel;
    private final String crdtId;
    private Set<TaggedElement> set;

    public ORSetCRDT(CRDTCommunicationInterface kernel, String crdtId) {
        this.kernel = kernel;
        this.crdtId = crdtId;
        this.set = new HashSet<>();
    }

    public ORSetCRDT(CRDTCommunicationInterface kernel, String crdtId, Set<TaggedElement> set) {
        this.kernel = kernel;
        this.crdtId = crdtId;
        this.set = new HashSet<>();
        this.set.addAll(set);
    }

    @Override
    public String getCrdtId() {
        return this.crdtId;
    }

    public synchronized boolean lookup(SerializableType elem) {
        for (TaggedElement taggedElement : this.set) {
            if (taggedElement.getValue().equals(elem))
                return true;
        }
        return false;
    }

    public synchronized Set<SerializableType> elements() {
        Set<SerializableType> elemSet = new HashSet<>();
        this.set.forEach(e -> elemSet.add(e.getValue()));
        return elemSet;
    }

    public synchronized void add(Host sender, SerializableType elem) {
        TaggedElement e = new TaggedElement(elem, UUID.randomUUID());
        Set<TaggedElement> toAdd = new HashSet<>();
        this.set.add(e);
        toAdd.add(e);
        Operation op = new SetOperation(SET_ADD, crdtId, CRDT_TYPE, toAdd);
        UUID id = UUID.randomUUID();
        logger.debug("Downstream add {} op for {} - {}", elem, crdtId, id);
        kernel.downstream(new DownstreamRequest(id, sender, op), (short)0);
    }

    public synchronized void remove(Host sender, SerializableType elem) {
        Set<TaggedElement> toRemove = new HashSet<>();
        Iterator<TaggedElement> it = this.set.iterator();

        while(it.hasNext()) {
            TaggedElement e = it.next();
            if(e.getValue().equals(elem)) {
                toRemove.add(e);
                it.remove();
            }
        }
        Operation op = new SetOperation(SET_REMOVE, crdtId, CRDT_TYPE, toRemove);
        UUID id = UUID.randomUUID();
        logger.debug("Downstream remove {} op for {} - {}", elem, crdtId, id);
        kernel.downstream(new DownstreamRequest(UUID.randomUUID(), sender, op), (short)0);
    }

    public synchronized void upstream(Operation op) {
        Set<TaggedElement> newSet = ((SetOperation)op).getSet();
        if (op.getOpType().equals(SET_ADD)) {
            this.set.addAll(newSet);
        }
        else if (op.getOpType().equals(SET_REMOVE)) {
            this.set.removeAll(newSet);
        }
    }

    @Override
    public synchronized void installState(KernelCRDT newCRDT) {
        Set<TaggedElement> newSet = ((ORSetCRDT) newCRDT).getTaggedElementSet();
        this.set.clear();
        this.set.addAll(newSet);
    }

    public static MyCRDTSerializer<SetCRDT> serializer = new MyCRDTSerializer<SetCRDT>() {
        @Override
        public void serialize(SetCRDT setCRDT, MySerializer[] serializers, ByteBuf out) throws IOException {
            out.writeInt(setCRDT.getCrdtId().getBytes().length);
            out.writeBytes(setCRDT.getCrdtId().getBytes());
            Set<TaggedElement> set = ((ORSetCRDT)setCRDT).getTaggedElementSet();
            out.writeInt(set.size());
            for (TaggedElement e : set) {
                TaggedElement.serializer.serialize(e, serializers, out);
            }
        }

        @Override
        public SetCRDT deserialize(CRDTCommunicationInterface kernel, MySerializer[] serializers, ByteBuf in) throws IOException {
            int size = in.readInt();
            byte[] crdtId = new byte[size];
            in.readBytes(crdtId);
            size = in.readInt();
            Set<TaggedElement> set = new HashSet<>();
            for(int i = 0; i < size; i++) {
                set.add(TaggedElement.serializer.deserialize(serializers, in));
            }
            return new ORSetCRDT(kernel, new String(crdtId), set);
        }
    };

    private Set<TaggedElement> getTaggedElementSet() {
        return this.set;
    }

}