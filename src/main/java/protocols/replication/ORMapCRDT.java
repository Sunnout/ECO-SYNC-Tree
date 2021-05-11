package protocols.replication;

import crdts.interfaces.MapCRDT;
import crdts.operations.MapOperation;
import crdts.operations.Operation;
import crdts.utils.TaggedElement;
import crdts.utils.VectorClock;
import datatypes.SerializableType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.replication.requests.DownstreamRequest;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The ORMap shares the same semantics as the ORSet, i.e. the elements can be added
 * and removed any number of times and the adds always win. In the case of concurrent
 * adds, the 2 new values are merged.
 */
public class ORMapCRDT implements MapCRDT, KernelCRDT {

    private static final Logger logger = LogManager.getLogger(ORMapCRDT.class);

    public enum MapOpType{
        PUT,
        DELETE
    }

    private static final String CRDT_TYPE = "or_map";
    private static final String MAP_PUT = "map_put";
    private static final String MAP_DELETE = "map_del";

    private final ReplicationKernel kernel;
    private final String crdtId;
    private Map<SerializableType, Set<TaggedElement>> map;

    public ORMapCRDT(ReplicationKernel kernel, String crdtId) {
        this.kernel = kernel;
        this.crdtId = crdtId;
        this.map = new ConcurrentHashMap<>();
    }

    @Override
    public String getCrdtId() {
        return this.crdtId;
    }

    public Set<SerializableType> get(SerializableType key) {
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

    public boolean contains(SerializableType key) {
        if(key == null)
            return false;

        // If the value is an empty set, the key is not in the map
        Set<TaggedElement> values = this.map.get(key);
        return values != null && !values.isEmpty();
    }

    public Set<SerializableType> keys() {
        Set<SerializableType> keySet = new HashSet<>();
        // If the value is an empty set, the key is not returned
        this.map.forEach((key, value) -> {
            if (!value.isEmpty())
                keySet.add(key);
        });
        return keySet;
    }

    public List<SerializableType> values() {
        Collection<Set<TaggedElement>> sets = this.map.values();
        List<SerializableType> values = new LinkedList<>();

        //TODO: java.util.ConcurrentModificationException
        sets.forEach(s ->
                s.forEach( elem ->
                        values.add(elem.getValue())));
        return values;
    }

    public void put(Host sender, SerializableType key, SerializableType value) {
        TaggedElement elem = new TaggedElement(value, UUID.randomUUID());
        Set<TaggedElement> toRemove = this.map.get(key);
        toRemove = checkForNullSet(toRemove);
        Set<TaggedElement> toAdd = new HashSet<>();
        toAdd.add(elem);
        this.map.put(key, toAdd);
        Operation op = new MapOperation(sender, 0, null, MAP_PUT, crdtId, CRDT_TYPE, key, elem, toRemove);
        UUID id = UUID.randomUUID();
        logger.debug("Downstream put {} {} op for {} - {}", key, value, crdtId, id);
        kernel.downstream(new DownstreamRequest(id, sender, op), (short)0);
    }

    public void delete(Host sender, SerializableType key) {
        Set<TaggedElement> toRemove = this.map.remove(key);
        toRemove = checkForNullSet(toRemove);
        Operation op = new MapOperation(sender, 0, null, MAP_DELETE, crdtId, CRDT_TYPE, key, null, toRemove);
        UUID id = UUID.randomUUID();
        logger.debug("Downstream delete {} op for {} - {}", key, crdtId, id);
        kernel.downstream(new DownstreamRequest(id, sender, op), (short)0);
    }

    public void upstream(Operation op) {
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

    private Set<TaggedElement> checkForNullSet(Set<TaggedElement> set) {
        return set == null ? new HashSet<>() : set;
    }

}
