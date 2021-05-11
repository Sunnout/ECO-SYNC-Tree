package crdts.interfaces;

import pt.unl.fct.di.novasys.network.data.Host;


public interface CounterCRDT extends GenericCRDT {

    int value();

    void increment(Host sender);

    void incrementBy(Host sender, int v);

    void decrement(Host sender);

    void decrementBy(Host sender, int v);

}
