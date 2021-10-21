package crdts.interfaces;

import datatypes.SerializableType;

import java.util.*;


public interface MapCRDT extends GenericCRDT {

    Set<SerializableType> get(SerializableType key);

    boolean contains(SerializableType key);

    Set<SerializableType> keys();

    List<SerializableType> values();

}
