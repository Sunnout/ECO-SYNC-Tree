package crdts.interfaces;


import datatypes.SerializableType;
import pt.unl.fct.di.novasys.network.data.Host;


public interface RegisterCRDT extends GenericCRDT {

    SerializableType value();

    void assign(Host sender, SerializableType value);

}
