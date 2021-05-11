package protocols.membership.statictree;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.membership.common.notifications.ChannelCreated;
import protocols.membership.common.notifications.NeighbourDown;
import protocols.membership.common.notifications.NeighbourUp;
import protocols.membership.full.timers.InfoTimer;
import protocols.membership.statictree.timers.ExitTimer;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.*;

public class StaticTreeMembership extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(StaticTreeMembership.class);

    //Protocol information, to register in babel
    public final static short PROTOCOL_ID = 490;
    public final static String PROTOCOL_NAME = "TreeMembership";

    private final Host self; //My own address/port
    private final Set<Host> membership; //Peers I am connected to
    private final Set<Host> pending; //Peers I am trying to connect to

    private final int nodeDegree; //param: number of connections of each node
    private final int size; //param: number nodes in the overlay
    private final int prepareTime;

    private final Random rnd;

    private final int channelId; //Id of the created channel

    public StaticTreeMembership(Properties props, Host self) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);

        this.self = self;
        this.membership = new HashSet<>();
        this.pending = new HashSet<>();

        this.rnd = new Random();

        //Get some configurations from the Properties object
        this.nodeDegree = Integer.parseInt(props.getProperty("node_degree", "2"));
        this.size = Integer.parseInt(props.getProperty("size", "10"));
        this.prepareTime = Integer.parseInt(props.getProperty("prepare_time"));


        String cMetricsInterval = props.getProperty("channel_metrics_interval", "10000"); //10 seconds

        //Create a properties object to setup channel-specific properties. See the channel description for more details.
        Properties channelProps = new Properties();
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, props.getProperty("address")); //The address to bind to
        channelProps.setProperty(TCPChannel.PORT_KEY, props.getProperty("port")); //The port to bind to
        channelProps.setProperty(TCPChannel.METRICS_INTERVAL_KEY, cMetricsInterval); //The interval to receive channel metrics
        channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000"); //Heartbeats interval for established connections
        channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000"); //Time passed without heartbeats until closing a connection
        channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000"); //TCP connect timeout
        channelId = createChannel(TCPChannel.NAME, channelProps); //Create the channel with the given properties

        /*---------------------- Register Message Serializers ---------------------- */

        /*---------------------- Register Message Handlers -------------------------- */

        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(InfoTimer.TIMER_ID, this::uponInfoTimer);
        registerTimerHandler(ExitTimer.TIMER_ID, this::uponExitTimer);


        /*-------------------- Register Channel Events ------------------------------- */
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
        registerChannelEventHandler(channelId, ChannelMetrics.EVENT_ID, this::uponChannelMetrics);
    }

    @Override
    public void init(Properties props) {
        //Inform the dissemination protocol about the channel we created in the constructor
        triggerNotification(new ChannelCreated(channelId));

        attemptConnections();

        //Setup the timer to display protocol information (also registered handler previously)
        int pMetricsInterval = Integer.parseInt(props.getProperty("protocol_metrics_interval", "10000"));
        if (pMetricsInterval > 0)
            setupPeriodicTimer(new InfoTimer(), pMetricsInterval, pMetricsInterval);

        setupTimer(new ExitTimer(), prepareTime * 1000);
    }

    /*--------------------------------- Messages ---------------------------------------- */

    /*--------------------------------- Timers ---------------------------------------- */

    //If we setup the InfoTimer in the constructor, this event will be triggered periodically.
    //We are simply printing some information to present during runtime.
    private void uponInfoTimer(InfoTimer timer, long timerId) {
        StringBuilder sb = new StringBuilder("Membership Metrics:\n");
        sb.append("Membership: ").append(membership).append("\n");
        sb.append("PendingMembership: ").append(pending).append("\n");
        //getMetrics returns an object with the number of events of each type processed by this protocol.
        //It may or may not be useful to you, but at least you know it exists.
        sb.append(getMetrics());
        logger.info(sb);
    }

    private void uponExitTimer(ExitTimer timer, long timerId) {
        attemptConnections();
    }

    /*--------------------------------- Auxiliary Methods ---------------------------------------- */

    //Gets a random element from the set of peers
    private Host getRandom(Set<Host> hostSet) {
        int idx = rnd.nextInt(hostSet.size());
        int i = 0;
        for (Host h : hostSet) {
            if (i == idx)
                return h;
            i++;
        }
        return null;
    }

    //Gets a random subset from the set of peers
    private static Set<Host> getRandomSubsetExcluding(Set<Host> hostSet, int sampleSize, Host exclude) {
        List<Host> list = new LinkedList<>(hostSet);
        list.remove(exclude);
        Collections.shuffle(list);
        return new HashSet<>(list.subList(0, Math.min(sampleSize, list.size())));
    }

    private void attemptConnections() {
        int startNode = nodeDegree * (self.getPort()%5000) + 5001;

        //Attempt to establish connections
        for(int i = startNode; i < startNode + nodeDegree; i++) {
            if(i > 5000 + size - 1)
                break;
            Host contactHost = new Host(self.getAddress(), i);
            //We add to the pending set until the connection is successful
            pending.add(contactHost);
            openConnection(contactHost);
        }
    }

    /* --------------------------------- TCPChannel Events ---------------------------- */

    //If a connection is successfully established, this event is triggered. In this protocol, we want to add the
    //respective peer to the membership, and inform the Dissemination protocol via a notification.
    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        Host peer = event.getNode();
        logger.info("Connection to {} is up", peer);
        pending.remove(peer);
        if (membership.add(peer)) {
            triggerNotification(new NeighbourUp(peer));
        }
    }

    //If an established connection is disconnected, remove the peer from the membership and inform the Dissemination
    //protocol. Alternatively, we could do smarter things like retrying the connection X times.
    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        Host peer = event.getNode();
        logger.info("Connection to {} is down cause {}", peer, event.getCause());
        membership.remove(event.getNode());
        triggerNotification(new NeighbourDown(event.getNode()));
    }

    //If a connection fails to be established, this event is triggered. In this protocol, we simply remove from the
    //pending set. Note that this event is only triggered while attempting a connection, not after connection.
    //Thus the peer will be in the pending set, and not in the membership (unless something is very wrong with our code)
    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        logger.info("Connection to {} failed cause: {}", event.getNode(), event.getCause());
        pending.remove(event.getNode());
    }

    //If someone established a connection to me, this event is triggered.
    //If we want to add the peer to the membership, we will establish our own outgoing connection.
    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        logger.info("Connection from {} is up", event.getNode());
        Host node = event.getNode();
        if(!membership.contains(node)) {
            pending.add(node);
            openConnection(node);
        }
    }

    //A connection someone established to me is disconnected.
    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        logger.info("Connection from {} is down, cause: {}", event.getNode(), event.getCause());
    }

    /* --------------------------------- Metrics ---------------------------- */

    //If we passed a value >0 in the METRICS_INTERVAL_KEY property of the channel, this event will be triggered
    //periodically by the channel. This is NOT a protocol timer, but a channel event.
    //Again, we are just showing some of the information you can get from the channel, and use how you see fit.
    //"getInConnections" and "getOutConnections" returns the currently established connection to/from me.
    //"getOldInConnections" and "getOldOutConnections" returns connections that have already been closed.
    private void uponChannelMetrics(ChannelMetrics event, int channelId) {
        StringBuilder sb = new StringBuilder("Channel Metrics:\n");
        sb.append("In channels:\n");
        event.getInConnections().forEach(c -> sb.append(String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s)\n",
                c.getPeer(), c.getSentAppMessages(), c.getSentAppBytes(), c.getReceivedAppMessages(),
                c.getReceivedAppBytes())));
        event.getOldInConnections().forEach(c -> sb.append(String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s) (old)\n",
                c.getPeer(), c.getSentAppMessages(), c.getSentAppBytes(), c.getReceivedAppMessages(),
                c.getReceivedAppBytes())));
        sb.append("Out channels:\n");
        event.getOutConnections().forEach(c -> sb.append(String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s)\n",
                c.getPeer(), c.getSentAppMessages(), c.getSentAppBytes(), c.getReceivedAppMessages(),
                c.getReceivedAppBytes())));
        event.getOldOutConnections().forEach(c -> sb.append(String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s) (old)\n",
                c.getPeer(), c.getSentAppMessages(), c.getSentAppBytes(), c.getReceivedAppMessages(),
                c.getReceivedAppBytes())));
        sb.setLength(sb.length() - 1);
        logger.info(sb);
    }
}
