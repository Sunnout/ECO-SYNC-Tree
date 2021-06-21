package protocols.membership.hyparview;

import protocols.membership.common.notifications.NeighbourDown;
import protocols.membership.common.notifications.NeighbourUp;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.membership.hyparview.messages.*;
import protocols.membership.hyparview.timers.HelloTimeout;
import protocols.membership.hyparview.timers.ShuffleTimer;
import protocols.membership.hyparview.utils.IView;
import protocols.membership.hyparview.utils.View;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;


public class HyParView extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(HyParView.class);

    public final static short PROTOCOL_ID = 400;
    public final static String PROTOCOL_NAME = "HyParView";

    protected int channelId;
    protected final Host myself;

    private static final int MAX_BACKOFF = 60000;

    private final short ARWL; //param: active random walk length
    private final short PRWL; //param: passive random walk length

    private final short shuffleTime; //param: timeout for shuffle
    private final short originalTimeout; //param: timeout for hello msgs
    private short timeout;

    private final short kActive; //param: number of active nodes to exchange on shuffle
    private final short kPassive; //param: number of passive nodes to exchange on shuffle

    protected IView active;
    protected IView passive;

    protected Set<Host> pending;
    private final Map<Short, Host[]> activeShuffles;

    private short seqNum = 0;

    protected final Random rnd;

    public HyParView(Properties properties, Host myself) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.myself = myself;

        int maxActive = Integer.parseInt(properties.getProperty("ActiveView", "4")); //param: maximum active nodes (degree of random overlay)
        int maxPassive = Integer.parseInt(properties.getProperty("PassiveView", "7")); //param: maximum passive nodes
        this.ARWL = Short.parseShort(properties.getProperty("ARWL", "4")); //param: active random walk length
        this.PRWL = Short.parseShort(properties.getProperty("PRWL", "2")); //param: passive random walk length

        this.shuffleTime = Short.parseShort(properties.getProperty("shuffleTime", "2000")); //param: timeout for shuffle
        this.timeout = this.originalTimeout = Short.parseShort(properties.getProperty("helloBackoff", "1000")); //param: timeout for hello msgs

        this.kActive = Short.parseShort(properties.getProperty("kActive", "2")); //param: number of active nodes to exchange on shuffle
        this.kPassive = Short.parseShort(properties.getProperty("kPassive", "3")); //param: number of passive nodes to exchange on shuffle

        this.rnd = new Random();
        this.active = new View(maxActive, myself, rnd);
        this.passive = new View(maxPassive, myself, rnd);

        this.pending = new HashSet<>();
        this.activeShuffles = new TreeMap<>();

        this.active.setOther(passive, pending);
        this.passive.setOther(active, pending);

        String cMetricsInterval = properties.getProperty("channel_metrics_interval", "10000"); // 10 seconds

        // Create a properties object to setup channel-specific properties. See the
        // channel description for more details.
        Properties channelProps = new Properties();
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, properties.getProperty("address")); // The address to bind to
        channelProps.setProperty(TCPChannel.PORT_KEY, properties.getProperty("port")); // The port to bind to
        channelProps.setProperty(TCPChannel.METRICS_INTERVAL_KEY, cMetricsInterval); // The interval to receive channel
        // metrics
        channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000"); // Heartbeats interval for established
        // connections
        channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000"); // Time passed without heartbeats until
        // closing a connection
        channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000"); // TCP connect timeout
        channelId = createChannel(TCPChannel.NAME, channelProps); // Create the channel with the given properties

        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(channelId, JoinMessage.MSG_CODE, JoinMessage.serializer);
        registerMessageSerializer(channelId, JoinReplyMessage.MSG_CODE, JoinReplyMessage.serializer);
        registerMessageSerializer(channelId, ForwardJoinMessage.MSG_CODE, ForwardJoinMessage.serializer);
        registerMessageSerializer(channelId, HelloMessage.MSG_CODE, HelloMessage.serializer);
        registerMessageSerializer(channelId, HelloReplyMessage.MSG_CODE, HelloReplyMessage.serializer);
        registerMessageSerializer(channelId, DisconnectMessage.MSG_CODE, DisconnectMessage.serializer);
        registerMessageSerializer(channelId, ShuffleMessage.MSG_CODE, ShuffleMessage.serializer);
        registerMessageSerializer(channelId, ShuffleReplyMessage.MSG_CODE, ShuffleReplyMessage.serializer);

        /*---------------------- Register Message Handlers -------------------------- */
        registerMessageHandler(channelId, JoinMessage.MSG_CODE, this::uponReceiveJoin);
        registerMessageHandler(channelId, JoinReplyMessage.MSG_CODE, this::uponReceiveJoinReply);
        registerMessageHandler(channelId, ForwardJoinMessage.MSG_CODE, this::uponReceiveForwardJoin);
        registerMessageHandler(channelId, HelloMessage.MSG_CODE, this::uponReceiveHello);
        registerMessageHandler(channelId, HelloReplyMessage.MSG_CODE, this::uponReceiveHelloReply);
        registerMessageHandler(channelId, DisconnectMessage.MSG_CODE, this::uponReceiveDisconnect, this::uponDisconnectSent);
        registerMessageHandler(channelId, ShuffleMessage.MSG_CODE, this::uponReceiveShuffle);
        registerMessageHandler(channelId, ShuffleReplyMessage.MSG_CODE, this::uponReceiveShuffleReply, this::uponShuffleReplySent);

        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(ShuffleTimer.TimerCode, this::uponShuffleTime);
        registerTimerHandler(HelloTimeout.TimerCode, this::uponHelloTimeout);

        /*-------------------- Register Channel Event ------------------------------- */
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
    }

    /*--------------------------------- Messages ---------------------------------------- */
    protected void handleDropFromActive(Host dropped) {
        if(dropped != null) {
            triggerNotification(new NeighbourDown(dropped));
            sendMessage(new DisconnectMessage(), dropped);
            logger.debug("Sent DisconnectMessage to {}", dropped);
            passive.addPeer(dropped);
            logger.trace("Added to {} passive{}", dropped, passive);
        }
    }

    private void uponReceiveJoin(JoinMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}", msg, from);
        openConnection(from);
        Host h = active.addPeer(from);
        logger.trace("Added to {} active{}", from, active);
        sendMessage( new JoinReplyMessage(), from);
        logger.debug("Sent JoinReplyMessage to {}", from);
        triggerNotification(new NeighbourUp(from));
        handleDropFromActive(h);

        for(Host peer : active.getPeers()) {
            if(!peer.equals(from)) {
                sendMessage(new ForwardJoinMessage(ARWL, from), peer);
                logger.debug("Sent ForwardJoinMessage to {}", peer);
            }
        }
    }

    private void uponReceiveJoinReply(JoinReplyMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}", msg, from);
        if(!active.containsPeer(from)) {
            passive.removePeer(from);
            pending.remove(from);

            openConnection(from);
            Host h = active.addPeer(from);
            logger.trace("Added to {} active{}", from, active);
            triggerNotification(new NeighbourUp(from));
            handleDropFromActive(h);
        }
    }

    private void uponReceiveForwardJoin(ForwardJoinMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}", msg, from);
        Host newHost = msg.getNewHost();
        if(msg.getTtl() == 0 || active.getPeers().size() == 1) {
            if(!newHost.equals(myself) && !active.containsPeer(newHost)) {
                passive.removePeer(newHost);
                pending.remove(newHost);
                openConnection(newHost);
                Host h = active.addPeer(newHost);
                logger.trace("Added to {} active{}", newHost, active);
                sendMessage(new JoinReplyMessage(), newHost);
                logger.debug("Sent JoinReplyMessage to {}", newHost);
                triggerNotification(new NeighbourUp(newHost));
                handleDropFromActive(h);
            }
        } else {
            if(msg.decrementTtl() == PRWL)  {
                passive.addPeer(newHost);
                logger.trace("Added to {} passive {}", newHost, passive);
            }
            Host next = active.getRandomDiff(from);
            if(next != null) {
                sendMessage(msg, next);
                logger.debug("Sent ForwardJoinMessage to {}", next);
            }
        }
    }

    private void uponReceiveHello(HelloMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}", msg, from);
        openConnection(from);

        if(msg.isPriority()) {
            sendMessage(new HelloReplyMessage(true), from);
            logger.debug("Sent HelloReplyMessage to {}", from);
            if(!active.containsPeer(from)) {
                pending.remove(from);
                logger.trace("Removed from {} pending{}", from, pending);
                passive.removePeer(from);
                logger.trace("Removed from {} passive{}", from, passive);
                Host h = active.addPeer(from);
                logger.trace("Added to {} active{}", from, active);
                triggerNotification(new NeighbourUp(from));
                handleDropFromActive(h);
            }

        } else {
            pending.remove(from);
            logger.trace("Removed from {} pending{}", from, pending);
            if(!active.fullWithPending(pending) || active.containsPeer(from)) {
                sendMessage(new HelloReplyMessage(true), from);
                logger.debug("Sent HelloReplyMessage to {}", from);
                if(!active.containsPeer(from)) {
                    passive.removePeer(from);
                    logger.trace("Removed from {} passive{}", from, passive);
                    active.addPeer(from);
                    logger.trace("Added to {} active{}", from, active);
                    triggerNotification(new NeighbourUp(from));
                }

            } else {
                sendMessage(new HelloReplyMessage(false), from, TCPChannel.CONNECTION_IN);
                logger.debug("Sent HelloReplyMessage to {}", from);
            }
        }
    }

    private void uponReceiveHelloReply(HelloReplyMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}", msg, from);
        pending.remove(from);
        logger.trace("Removed from {} pending{}", from, pending);
        if(msg.isTrue()) {
            if(!active.containsPeer(from)) {
                timeout = originalTimeout;
                openConnection(from);
                Host h = active.addPeer(from);
                logger.trace("Added to {} active{}", from, active);
                triggerNotification(new NeighbourUp(from));
                handleDropFromActive(h);
            }
        } else if(!active.containsPeer(from)){
            passive.addPeer(from);
            closeConnection(from);
            logger.trace("Added to {} passive{}", from, passive);
            if(!active.fullWithPending(pending)) {
                setupTimer(new HelloTimeout(), timeout);
            }
        }
    }

    protected void uponReceiveDisconnect(DisconnectMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}", msg, from);
        if(active.containsPeer(from)) {
            active.removePeer(from);
            logger.debug("Removed from {} active{}", from, active);
            handleDropFromActive(from);

            if(active.getPeers().isEmpty()) {
                timeout = originalTimeout;
            }

            if(!active.fullWithPending(pending)){
                setupTimer(new HelloTimeout(), timeout);
            }
        }
    }

    private void uponDisconnectSent(DisconnectMessage msg, Host host, short destProto, int channelId) {
        logger.trace("Sent {} to {}", msg, host);
        closeConnection(host);
    }

    private void uponReceiveShuffle(ShuffleMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}", msg, from);
        openConnection(from);

        msg.decrementTtl();
        if(msg.getTtl() > 0 && active.getPeers().size() > 1) {
            Host next = active.getRandomDiff(from);
            sendMessage(msg, next);
            logger.debug("Sent ShuffleMessage to {}", next);
        } else if(!msg.getOrigin().equals(myself)) {
            logger.trace("Processing {}, passive{}", msg, passive);
            Set<Host> peers = new HashSet<>();
            peers.addAll(active.getRandomSample(msg.getFullSample().size()));
            Host[] hosts = peers.toArray(new Host[0]);
            int i = 0;
            for (Host host : msg.getFullSample()) {
                if (!host.equals(myself) && !active.containsPeer(host) && passive.isFull() && i < peers.size()) {
                    passive.removePeer(hosts[i]);
                    i++;
                }
                passive.addPeer(host);
            }
            logger.trace("After Passive{}", passive);
            sendMessage(new ShuffleReplyMessage(peers, msg.getSeqnum()), msg.getOrigin());
            logger.debug("Sent ShuffleReplyMessage to {}", msg.getOrigin());
        } else
            activeShuffles.remove(msg.getSeqnum());
    }

    private void uponShuffleReplySent(ShuffleReplyMessage msg, Host host, short destProto, int channelId) {
        if(!active.containsPeer(host) && !pending.contains(host)) {
            logger.trace("Disconnecting from {} after shuffleReply", host);
            closeConnection(host);
        }
    }

    private void uponReceiveShuffleReply(ShuffleReplyMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}", msg, from);
        Host[] sent = activeShuffles.remove(msg.getSeqnum());
        List<Host> sample = msg.getSample();
        sample.add(from);
        int i = 0;
        logger.trace("Processing {}, passive{}", msg, passive);
        for (Host h : sample) {
            if(!h.equals(myself) && !active.containsPeer(h) && passive.isFull() && i < sent.length) {
                passive.removePeer(sent[i]);
                i ++;
            }
            passive.addPeer(h);
        }
        logger.trace("After Passive{}", passive);
    }

    /*--------------------------------- Timers ---------------------------------------- */
    private void uponShuffleTime(ShuffleTimer timer, long timerId) {
        if(!active.fullWithPending(pending)){
            setupTimer(new HelloTimeout(), timeout);
        }

        Host h = active.getRandom();

        if(h != null) {
            Set<Host> peers = new HashSet<>();
            peers.addAll(active.getRandomSample(kActive));
            peers.addAll(passive.getRandomSample(kPassive));
            activeShuffles.put(seqNum, peers.toArray(new Host[0]));
            sendMessage(new ShuffleMessage(myself, peers, PRWL, seqNum), h);
            logger.debug("Sent ShuffleMessage to {}", h);
            seqNum = (short) ((short) (seqNum % Short.MAX_VALUE) + 1);
        }
    }

    private void uponHelloTimeout(HelloTimeout timer, long timerId) {
        if(!active.fullWithPending(pending)){
            Host h = passive.dropRandom();
            if(h != null && pending.add(h)) {
                openConnection(h);
                logger.trace("Sending HelloMessage to {}, pending {}, active {}, passive {}", h, pending, active, passive);
                sendMessage(new HelloMessage(getPriority()), h);
                logger.debug("Sent HelloMessage to {}", h);
                timeout = (short) (Math.min(timeout * 2, MAX_BACKOFF));
            } else if(h != null)
                passive.addPeer(h);
        }
    }

    private boolean getPriority() {
        return active.getPeers().size() + pending.size() == 1;
    }

    /* --------------------------------- Channel Events ---------------------------- */

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        logger.debug("Host {} is down, active{}, cause: {}", event.getNode(), active, event.getCause());
        if(active.removePeer(event.getNode())) {
            triggerNotification(new NeighbourDown(event.getNode()));
            if(!active.fullWithPending(pending)){
                setupTimer(new HelloTimeout(), timeout);
            }
        } else
            pending.remove(event.getNode());
    }

    private void uponOutConnectionFailed(OutConnectionFailed event, int channelId) {
        logger.debug("Connection to host {} failed, cause: {}", event.getNode(), event.getCause());
        if(active.removePeer(event.getNode())) {
            triggerNotification(new NeighbourDown(event.getNode()));
            if(!active.fullWithPending(pending)){
                setupTimer(new HelloTimeout(), timeout);
            }
        } else
            pending.remove(event.getNode());
    }

    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        logger.trace("Host (out) {} is up", event.getNode());
    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        logger.trace("Host (in) {} is up", event.getNode());
    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        logger.trace("Connection from host {} is down, active{}, cause: {}", event.getNode(), active, event.getCause());
    }

    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {
        // If there is a contact node, attempt to establish connection
        if (props.containsKey("contact")) {
            try {
                logger.debug("Trying to reach contact node");
                String contact = props.getProperty("contact");
                String[] hostElems = contact.split(":");
                Host contactHost = new Host(InetAddress.getByName(hostElems[0]), Short.parseShort(hostElems[1]));
                openConnection(contactHost);
                JoinMessage m = new JoinMessage();
                sendMessage(m, contactHost);
                logger.info("Sent JoinMessage to {}", contactHost);
                logger.trace("Sent " + m + " to " + contactHost);
            } catch (Exception e) {
                logger.error("Invalid contact on configuration: '" + props.getProperty("contact"));
                e.printStackTrace();
                System.exit(-1);
            }
        } else
            logger.info("No contact node");

        setupPeriodicTimer(new ShuffleTimer(), this.shuffleTime, this.shuffleTime);
    }
}
