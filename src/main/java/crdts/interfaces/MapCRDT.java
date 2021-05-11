package crdts.interfaces;

import datatypes.SerializableType;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.*;


public interface MapCRDT extends GenericCRDT {

    Set<SerializableType> get(SerializableType key);

    boolean contains(SerializableType key);

    Set<SerializableType> keys();

    List<SerializableType> values();

    void put(Host sender, SerializableType key, SerializableType value);

    void delete(Host sender, SerializableType key);

}
