package crdts.interfaces;

import datatypes.SerializableType;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.Set;


public interface SetCRDT extends GenericCRDT {

    boolean lookup(SerializableType elem);

    Set<SerializableType> elements();

    void add(Host sender, SerializableType elem);

    void remove(Host sender, SerializableType elem);

}
