package protocols.replication;

import crdts.interfaces.SetCRDT;
import crdts.operations.Operation;
import crdts.operations.SetOperation;
import crdts.utils.TaggedElement;
import datatypes.SerializableType;
import io.netty.buffer.ByteBuf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import serializers.CRDTSerializer;
import serializers.MySerializer;

import java.io.IOException;
import java.util.*;


public class ORSetCRDT implements SetCRDT, KernelCRDT {

    private static final Logger logger = LogManager.getLogger(ORSetCRDT.class);

    private static final String CRDT_TYPE = "or_set";
    private static final String SET_ADD = "set_add";
    private static final String SET_REMOVE = "set_rem";

    public enum SetOpType{
        ADD,
        REMOVE
    }

    private final String crdtId;
    private Set<TaggedElement> set;

    public ORSetCRDT(String crdtId) {
        this.crdtId = crdtId;
        this.set = new HashSet<>();
    }

    public ORSetCRDT(String crdtId, Set<TaggedElement> set) {
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

    public synchronized SetOperation add(SerializableType elem) {
        TaggedElement e = new TaggedElement(elem, UUID.randomUUID());
        Set<TaggedElement> toAdd = new HashSet<>();
        this.set.add(e);
        toAdd.add(e);
        return new SetOperation(SET_ADD, crdtId, CRDT_TYPE, toAdd);
    }

    public synchronized SetOperation remove(SerializableType elem) {
        Set<TaggedElement> toRemove = new HashSet<>();
        Iterator<TaggedElement> it = this.set.iterator();

        while(it.hasNext()) {
            TaggedElement e = it.next();
            if(e.getValue().equals(elem)) {
                toRemove.add(e);
                it.remove();
            }
        }
        return new SetOperation(SET_REMOVE, crdtId, CRDT_TYPE, toRemove);
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

    public static CRDTSerializer<SetCRDT> serializer = new CRDTSerializer<SetCRDT>() {
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
        public SetCRDT deserialize(MySerializer[] serializers, ByteBuf in) throws IOException {
            int size = in.readInt();
            byte[] crdtId = new byte[size];
            in.readBytes(crdtId);
            size = in.readInt();
            Set<TaggedElement> set = new HashSet<>();
            for(int i = 0; i < size; i++) {
                set.add(TaggedElement.serializer.deserialize(serializers, in));
            }
            return new ORSetCRDT(new String(crdtId), set);
        }
    };

    private Set<TaggedElement> getTaggedElementSet() {
        return this.set;
    }

}