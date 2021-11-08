package protocols.replication.crdts.interfaces;

import protocols.replication.crdts.datatypes.SerializableType;

import java.util.*;


public interface MapCRDT extends GenericCRDT {

    Set<SerializableType> get(SerializableType key);

    boolean contains(SerializableType key);

    Set<SerializableType> keys();

    List<SerializableType> values();

}
