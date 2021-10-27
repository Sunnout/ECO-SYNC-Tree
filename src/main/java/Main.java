import java.net.InetAddress;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import protocols.broadcast.flood.FloodBroadcast;
import protocols.broadcast.periodicpull.PeriodicPullBroadcast;
import protocols.broadcast.plumtree.PlumTree;
import protocols.membership.hyparview.HyParView;
import protocols.replication.ReplicationKernel;
import pt.unl.fct.di.novasys.babel.core.Babel;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.network.data.Host;
import protocols.apps.CRDTApp;
import utils.InterfaceToIp;


public class Main {

    //Sets the log4j (logging library) configuration file
    static {
        System.setProperty("log4j.configurationFile", "log4j2.xml");
    }

    //Creates the logger object
    private static final Logger logger = LogManager.getLogger(Main.class);

    //Default babel configuration file (can be overridden by the "-config" launch argument)
    private static final String DEFAULT_CONF = "config.properties";

    public static void main(String[] args) throws Exception {

        //Get the (singleton) babel instance
        Babel babel = Babel.getInstance();

        //Loads properties from the configuration file, and merges them with properties passed in the launch arguments
        Properties props = Babel.loadConfig(args, DEFAULT_CONF);

        //If you pass an interface name in the properties (either file or arguments), this wil get the IP of that interface
        //and create a property "address=ip" to be used later by the channels.
        InterfaceToIp.addInterfaceIp(props);

        //The Host object is an address/port pair that represents a network host. It is used extensively in babel
        //It implements equals and hashCode, and also includes a serializer that makes it easy to use in network messages
        Host myself_membership =  new Host(InetAddress.getByName(props.getProperty("address")),
                Integer.parseInt(props.getProperty("port")));

        Host myself =  new Host(InetAddress.getByName(props.getProperty("address")),
                Integer.parseInt(props.getProperty("bcast_port")));

        logger.warn("Hello, I am {} {}", myself_membership, myself);

        String bcast_protocol = props.getProperty("bcast_protocol");

        CRDTApp crdtApp;
        GenericProtocol replicationKernel;
        GenericProtocol broadcast;
        GenericProtocol membership;

        switch(bcast_protocol) {
            case "plumtree":
                crdtApp = new CRDTApp(props, myself, ReplicationKernel.PROTOCOL_ID, PlumTree.PROTOCOL_ID);
                replicationKernel = new ReplicationKernel(myself, PlumTree.PROTOCOL_ID);
                broadcast = new PlumTree(props, myself);
                membership = new HyParView(props, myself_membership);
                registerAndStartProtocols(babel, crdtApp, replicationKernel, broadcast, membership, props);
                break;

            case "flood":
                crdtApp = new CRDTApp(props, myself, ReplicationKernel.PROTOCOL_ID, FloodBroadcast.PROTOCOL_ID);
                replicationKernel = new ReplicationKernel(myself, FloodBroadcast.PROTOCOL_ID);
                broadcast = new FloodBroadcast(props, myself);
                membership = new HyParView(props, myself_membership);
                registerAndStartProtocols(babel, crdtApp, replicationKernel, broadcast, membership, props);
                break;

            case "periodicpull":
                crdtApp = new CRDTApp(props, myself, ReplicationKernel.PROTOCOL_ID, PeriodicPullBroadcast.PROTOCOL_ID);
                replicationKernel = new ReplicationKernel(myself, PeriodicPullBroadcast.PROTOCOL_ID);
                broadcast = new PeriodicPullBroadcast(props, myself);
                membership = new HyParView(props, myself_membership);
                registerAndStartProtocols(babel, crdtApp, replicationKernel, broadcast, membership, props);
                break;

            default:
                logger.error("There is no broadcast protocol with name {}", bcast_protocol);
                break;
        }
    }

    private static final void registerAndStartProtocols(Babel babel, CRDTApp crdtApp, GenericProtocol replicationKernel,
                                                        GenericProtocol broadcast, GenericProtocol membership, Properties props ) throws Exception {

        //Register the protocols
        babel.registerProtocol(crdtApp);
        babel.registerProtocol(replicationKernel);
        babel.registerProtocol(broadcast);
        babel.registerProtocol(membership);

        //Init the protocols
        crdtApp.init(props);
        replicationKernel.init(props);
        broadcast.init(props);
        membership.init(props);

        //Start babel and protocol threads
        babel.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> logger.warn("Goodbye")));
    }
}