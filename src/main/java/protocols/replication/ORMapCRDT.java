package protocols.replication;

import protocols.replication.crdts.interfaces.MapCRDT;
import protocols.replication.crdts.operations.MapOperation;
import protocols.replication.crdts.operations.Operation;
import protocols.replication.crdts.utils.TaggedElement;
import protocols.replication.crdts.datatypes.SerializableType;
import io.netty.buffer.ByteBuf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.replication.crdts.serializers.CRDTSerializer;
import protocols.replication.crdts.serializers.MySerializer;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The ORMap shares the same semantics as the ORSet, i.e. the elements can be added
 * and removed any number of times and the adds always win. In the case of concurrent
 * adds, the 2 new values are merged.
 */
public class ORMapCRDT implements MapCRDT, KernelCRDT {

    private static final Logger logger = LogManager.getLogger(ORMapCRDT.class);

    private static final String CRDT_TYPE = "or_map";
    private static final String MAP_PUT = "map_put";
    private static final String MAP_DELETE = "map_del";

    public enum MapOpType{
        PUT,
        DELETE
    }

    private final String crdtId;
    private Map<SerializableType, Set<TaggedElement>> map;

    public ORMapCRDT(String crdtId) {
        this.crdtId = crdtId;
        this.map = new ConcurrentHashMap<>();
    }

    public ORMapCRDT(String crdtId, Map<SerializableType, Set<TaggedElement>> map) {
        this.crdtId = crdtId;
        this.map = new ConcurrentHashMap<>();
        this.map.putAll(map);
    }

    @Override
    public String getCrdtId() {
        return this.crdtId;
    }

    public synchronized Set<SerializableType> get(SerializableType key) {
        if(key == null)
            return null;

        Set<TaggedElement> elements = this.map.get(key);
        // If the value is an empty set, there is no mapping for the key
        if(elements == null || elements.isEmpty())
            return null;
        else {
            Set<SerializableType> values = new HashSet<>();
            elements.forEach(e -> values.add(e.getValue()));
            return values;
        }
    }

    public synchronized boolean contains(SerializableType key) {
        if(key == null)
            return false;

        // If the value is an empty set, the key is not in the map
        Set<TaggedElement> values = this.map.get(key);
        return values != null && !values.isEmpty();
    }

    public synchronized Set<SerializableType> keys() {
        Set<SerializableType> keySet = new HashSet<>();
        // If the value is an empty set, the key is not returned
        this.map.forEach((key, value) -> {
            if (!value.isEmpty())
                keySet.add(key);
        });
        return keySet;
    }

    public synchronized List<SerializableType> values() {
        Collection<Set<TaggedElement>> sets = this.map.values();
        List<SerializableType> values = new LinkedList<>();
        sets.forEach(s ->
                s.forEach( elem ->
                        values.add(elem.getValue())));
        return values;
    }

    public synchronized MapOperation putOperation(SerializableType key, SerializableType value) {
        TaggedElement elem = new TaggedElement(value, UUID.randomUUID());
        Set<TaggedElement> toRemove = this.map.get(key);
        toRemove = checkForNullSet(toRemove);
        return new MapOperation(MAP_PUT, crdtId, CRDT_TYPE, key, elem, toRemove);
    }

    public synchronized MapOperation deleteOperation(SerializableType key) {
        Set<TaggedElement> toRemove = this.map.get(key);
        toRemove = checkForNullSet(toRemove);
        return new MapOperation(MAP_DELETE, crdtId, CRDT_TYPE, key, null, toRemove);
    }

    public synchronized void upstream(Operation op) {
        Set<TaggedElement> toRemove = ((MapOperation)op).getSet();
        SerializableType key = ((MapOperation)op).getKey();
        Set<TaggedElement> currentSet = this.map.remove(key);
        currentSet = checkForNullSet(currentSet);
        currentSet.removeAll(toRemove);

        if(op.getOpType().equals(MAP_PUT)) {
            currentSet.add(((MapOperation) op).getElement());
            this.map.put(key, currentSet);
        } else if(op.getOpType().equals(MAP_DELETE) && !currentSet.isEmpty()) {
            this.map.put(key, currentSet);
        }
    }

    @Override
    public synchronized void installState(KernelCRDT newCRDT) {
        Map<SerializableType, Set<TaggedElement>> newMap = ((ORMapCRDT) newCRDT).getMap();
        this.map.clear();
        this.map.putAll(newMap);
    }

    public static CRDTSerializer<MapCRDT> serializer = new CRDTSerializer<MapCRDT>() {
        @Override
        public void serialize(MapCRDT mapCRDT, MySerializer[] serializers, ByteBuf out) throws IOException {
            out.writeInt(mapCRDT.getCrdtId().getBytes().length);
            out.writeBytes(mapCRDT.getCrdtId().getBytes());
            Map<SerializableType, Set<TaggedElement>> map = ((ORMapCRDT)mapCRDT).getMap();
            out.writeInt(map.size());
            for (Map.Entry<SerializableType, Set<TaggedElement>> entry : map.entrySet()) {
                serializers[0].serialize(entry.getKey(), out);
                Set<TaggedElement> set = entry.getValue();
                out.writeInt(set.size());
                for (TaggedElement e : set) {
                    TaggedElement.serializer.serialize(e, getValueSerializer(serializers), out);
                }
            }
        }

        @Override
        public MapCRDT deserialize(MySerializer[] serializers, ByteBuf in) throws IOException {
            int size = in.readInt();
            byte[] crdtId = new byte[size];
            in.readBytes(crdtId);
            size = in.readInt();
            Map<SerializableType, Set<TaggedElement>> map = new HashMap<>();
            for(int i = 0; i < size; i++) {
                SerializableType key = (SerializableType) serializers[0].deserialize(in);
                int setSize = in.readInt();
                Set<TaggedElement> set = new HashSet<>();
                for (int j = 0; j < setSize; j++) {
                    set.add(TaggedElement.serializer.deserialize(getValueSerializer(serializers), in));
                }
                map.put(key, set);
            }
            return new ORMapCRDT(new String(crdtId), map);
        }
    };

    private static MySerializer[] getValueSerializer(MySerializer[] serializers) {
        MySerializer[] array = new MySerializer[1];
        array[0] = serializers[1];
        return array;
    }

    private Set<TaggedElement> checkForNullSet(Set<TaggedElement> set) {
        return set == null ? new HashSet<>() : set;
    }

    private Map<SerializableType, Set<TaggedElement>> getMap() {
        return this.map;
    }

}