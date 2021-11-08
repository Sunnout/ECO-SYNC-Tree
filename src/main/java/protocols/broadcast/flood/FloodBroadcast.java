package protocols.broadcast.flood;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.broadcast.common.messages.SendVectorClockMessage;
import protocols.broadcast.common.messages.SynchronizationMessage;
import protocols.broadcast.common.messages.VectorClockMessage;
import protocols.broadcast.common.requests.BroadcastRequest;
import protocols.broadcast.common.notifications.DeliverNotification;
import protocols.broadcast.common.utils.CommunicationCostCalculator;
import protocols.broadcast.common.utils.MultiFileManager;
import protocols.broadcast.common.utils.VectorClock;
import protocols.broadcast.common.timers.ReconnectTimeout;
import protocols.broadcast.common.messages.GossipMessage;
import protocols.broadcast.flood.utils.FloodStats;
import protocols.broadcast.plumtree.utils.IncomingSync;
import protocols.broadcast.plumtree.utils.OutgoingSync;
import protocols.membership.common.notifications.NeighbourDown;
import protocols.membership.common.notifications.NeighbourUp;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.*;

public class FloodBroadcast extends CommunicationCostCalculator {
    private static final Logger logger = LogManager.getLogger(FloodBroadcast.class);

    public final static short PROTOCOL_ID = 1500;
    public final static String PROTOCOL_NAME = "BCAST-Flood";

    protected int channelId;
    private final Host myself;
    private final static int PORT_MAPPING = 1000;
    private final static int SECONDS_TO_MILLIS = 1000;

    private final long reconnectTimeout;

    private final Set<Host> partialView;
    private final Set<Host> neighbours;
    private final Set<UUID> received;

    private final Set<OutgoingSync> outgoingSyncs; // Hosts we have asked for vcs
    private IncomingSync incomingSync; // Host that we sent our vc to
    private final Queue<IncomingSync> pendingIncomingSyncs; // Queue of pending incoming syncs

    public static VectorClock vectorClock; // Local vector clock
    private int seqNumber; // Counter of local operations

    private final MultiFileManager fileManager;

    private final FloodStats stats;


    /*--------------------------------- Initialization ---------------------------------------- */

    public FloodBroadcast(Properties properties, Host myself) throws HandlerRegistrationException, IOException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.myself = myself;

        long garbageCollectionTimeout = Long.parseLong(properties.getProperty("garbage_collection_timeout", "15")) * SECONDS_TO_MILLIS;
        long garbageCollectionTTL = Long.parseLong(properties.getProperty("garbage_collection_ttl", "60")) * SECONDS_TO_MILLIS;
        this.reconnectTimeout = Long.parseLong(properties.getProperty("reconnect_timeout", "500"));

        this.partialView = new HashSet<>();
        this.neighbours = new HashSet<>();
        this.received = new HashSet<>();

        this.outgoingSyncs = new HashSet<>();
        this.incomingSync = new IncomingSync(null, null);
        this.pendingIncomingSyncs = new LinkedList<>();

        vectorClock = new VectorClock(myself);

        int indexSpacing = Integer.parseInt(properties.getProperty("index_spacing", "100"));
        this.fileManager = new MultiFileManager(garbageCollectionTimeout, garbageCollectionTTL, indexSpacing, myself);

        this.stats = new FloodStats();


        String cMetricsInterval = properties.getProperty("bcast_channel_metrics_interval", "10000"); // 10 seconds

        // Create a properties object to set up channel-specific properties. See the
        // channel description for more details.
        Properties channelProps = new Properties();
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, properties.getProperty("address")); // The address to bind to
        channelProps.setProperty(TCPChannel.PORT_KEY, properties.getProperty("bcast_port")); // The port to bind to
        channelProps.setProperty(TCPChannel.METRICS_INTERVAL_KEY, cMetricsInterval); // The interval to receive channel
        // metrics
        channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000"); // Heartbeats interval for established
        // connections
        channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000"); // Time passed without heartbeats until
        // closing a connection
        channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000"); // TCP connect timeout
        channelId = createChannel(TCPChannel.NAME, channelProps); // Create the channel with the given properties

        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(ReconnectTimeout.TIMER_ID, this::uponReconnectTimeout);

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(BroadcastRequest.REQUEST_ID, this::uponBroadcastRequest);

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(NeighbourUp.NOTIFICATION_ID, this::uponNeighbourUp);
        subscribeNotification(NeighbourDown.NOTIFICATION_ID, this::uponNeighbourDown);

        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(channelId, GossipMessage.MSG_ID, GossipMessage.serializer);

        registerMessageSerializer(channelId, SendVectorClockMessage.MSG_ID, SendVectorClockMessage.serializer);
        registerMessageSerializer(channelId, VectorClockMessage.MSG_ID, VectorClockMessage.serializer);
        registerMessageSerializer(channelId, SynchronizationMessage.MSG_ID, SynchronizationMessage.serializer);

        /*---------------------- Register Message Handlers -------------------------- */
        registerMessageHandler(channelId, GossipMessage.MSG_ID, this::uponReceiveGossipMsg, this::onMessageFailed);

        registerMessageHandler(channelId, SendVectorClockMessage.MSG_ID, this::uponReceiveSendVectorClockMsg, this::onMessageFailed);
        registerMessageHandler(channelId, VectorClockMessage.MSG_ID, this::uponReceiveVectorClockMsg, this::onMessageFailed);
        registerMessageHandler(channelId, SynchronizationMessage.MSG_ID, this::uponReceiveSynchronizationMsg, this::onMessageFailed);

        /*-------------------- Register Channel Event ------------------------------- */
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
        registerChannelEventHandler(channelId, ChannelMetrics.EVENT_ID, this::uponChannelMetrics);
    }

    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {

    }


    /*--------------------------------- Requests ---------------------------------------- */

    private void uponBroadcastRequest(BroadcastRequest request, short sourceProto) {
        UUID mid = request.getMsgId();
        byte[] content = request.getMsg();
        logger.debug("Propagating my {} to {}", mid, neighbours);
        GossipMessage msg = new GossipMessage(mid, myself, ++seqNumber, content);
        logger.info("SENT {}", mid);
        uponReceiveGossipMsg(msg, myself, getProtoId(), -1);
    }


    /*--------------------------------- Messages ---------------------------------------- */

    private void uponReceiveGossipMsg(GossipMessage msg, Host from, short sourceProto, int channelId) {
        this.stats.incrementReceivedFlood();
        UUID mid = msg.getMid();
        logger.debug("Received {} from {}. Is from sync {}", mid, from, false);
        if (received.add(mid)) {
            handleGossipMessage(msg, from, false);
        } else {
            logger.info("DUPLICATE from {}", from);
            this.stats.incrementReceivedDupesFlood();
        }
    }

    private void uponReceiveVectorClockMsg(VectorClockMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}.", msg, from);
        this.stats.incrementReceivedVC();
        SynchronizationMessage synchronizationMsg = new SynchronizationMessage(msg.getMid(), null,
                this.fileManager.readSyncOpsFromFile(msg.getVectorClock(), vectorClock));
        sendMessage(synchronizationMsg, from);
        this.stats.incrementSentSyncOps();
        this.stats.incrementSentSyncFloodBy(synchronizationMsg.getMsgs().size());
        logger.debug("Sent {} to {}", msg, from);

        if (neighbours.add(from)) {
            logger.debug("Added {} to neighbours {}", from, neighbours);
        }
    }

    private void uponReceiveSendVectorClockMsg(SendVectorClockMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}.", msg, from);
        this.stats.incrementReceivedSendVC();
        UUID mid = msg.getMid();
        Host currentPending = incomingSync.getHost();
        if (currentPending == null) {
            incomingSync = new IncomingSync(from, mid);
            logger.debug("Added {} to incomingSync; pendingIncomingSyncs {} ", from, pendingIncomingSyncs);
            VectorClockMessage vectorClockMessage = new VectorClockMessage(mid, new VectorClock(vectorClock.getClock()));
            sendMessage(vectorClockMessage, from, TCPChannel.CONNECTION_IN);
            this.stats.incrementSentVC();
            logger.debug("Sent {} to {}", vectorClockMessage, from);
        } else {
            pendingIncomingSyncs.add(new IncomingSync(from, mid));
            logger.debug("Added {} to pendingIncomingSyncs {}", from, pendingIncomingSyncs);
        }
    }

    private void uponReceiveSynchronizationMsg(SynchronizationMessage msg, Host from, short sourceProto, int channelId) {
        this.stats.incrementReceivedSyncOps();
        logger.debug("Received {} from {}", msg, from);

        try {
            for (byte[] serMsg : msg.getMsgs()) {
                this.stats.incrementReceivedSyncFlood();

                DataInputStream dis = new DataInputStream(new ByteArrayInputStream(serMsg));
                GossipMessage gossipMessage = GossipMessage.deserialize(dis);
                UUID mid = gossipMessage.getMid();

                if (received.add(mid)) {
                    handleGossipMessage(gossipMessage, from, true);
                } else {
                    logger.info("DUPLICATE from {}", from);
                    this.stats.incrementReceivedDupesSyncFlood();
                }
            }
            logger.debug("Received sync ops. Sync {} ENDED", msg.getMid());
            tryNextIncomingSync();
        } catch (IOException e) {
            logger.error("Sync message handling error", e);
        }
    }

    private void onMessageFailed(ProtoMessage protoMessage, Host host, short destProto, Throwable reason, int channel) {
        logger.warn("Message failed to " + host + ", " + protoMessage + ": " + reason.getMessage());
    }


    /*--------------------------------- Timers ---------------------------------------- */

    private void uponReconnectTimeout(ReconnectTimeout timeout, long timerId) {
        Host neighbour = timeout.getHost();
        if(partialView.contains(neighbour)) {
            logger.debug("Reconnecting with {}", neighbour);
            openConnection(neighbour);
        } else {
            logger.debug("Not reconnecting because {} is down", neighbour);
        }
    }


    /*--------------------------------- Notifications ---------------------------------------- */

    private void uponNeighbourUp(NeighbourUp notification, short sourceProto) {
        Host tmp = notification.getNeighbour();
        Host neighbour = new Host(tmp.getAddress(),tmp.getPort() + PORT_MAPPING);

        if (partialView.add(neighbour)) {
            logger.debug("Added {} to partial view due to up {}", neighbour, partialView);
        } else {
            logger.error("Tried to add {} to partial view but is already there {}", neighbour, partialView);
        }

        openConnection(neighbour);
    }

    private void uponNeighbourDown(NeighbourDown notification, short sourceProto) {
        Host tmp = notification.getNeighbour();
        Host neighbour = new Host(tmp.getAddress(),tmp.getPort() + PORT_MAPPING);

        if (partialView.remove(neighbour)) {
            logger.debug("Removed {} from partial view due to down {}", neighbour, partialView);
        }

        if (neighbours.remove(neighbour)) {
            logger.debug("Removed {} from neighbours due to down {}", neighbour, neighbours);
        }

        if (removeFromPendingIncomingSyncs(neighbour)) {
            logger.debug("Removed {} from pending due to down {}", neighbour, pendingIncomingSyncs);
        }

        if (outgoingSyncs.remove(new OutgoingSync(neighbour))) {
            logger.debug("Removed {} from onGoingSyncs due to down {}", neighbour, outgoingSyncs);
        }

        if (neighbour.equals(incomingSync.getHost())) {
            logger.debug("Removed {} from current pending due to down", neighbour);
            tryNextIncomingSync();
        }
        closeConnection(neighbour);
    }


    /* --------------------------------- Channel Events ---------------------------- */

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        Host host = event.getNode();
        logger.debug("Host {} is down, cause: {}", host, event.getCause());

        if (neighbours.remove(host)) {
            logger.debug("Removed {} from neighbours due to flood down {}", host, neighbours);
        }

        if (removeFromPendingIncomingSyncs(host)) {
            logger.debug("Removed {} from pending due to flood down {}", host, pendingIncomingSyncs);
        }

        if (outgoingSyncs.remove(new OutgoingSync(host))) {
            logger.debug("Removed {} from onGoingSyncs due to flood down {}", host, outgoingSyncs);
        }

        if (host.equals(incomingSync.getHost())) {
            logger.debug("Removed {} from current pending due to flood down", host);
            tryNextIncomingSync();
        }

        if(partialView.contains(host)) {
            setupTimer(new ReconnectTimeout(host), reconnectTimeout);
        }
    }

    @SuppressWarnings("rawtypes")
    private void uponOutConnectionFailed(OutConnectionFailed event, int channelId) {
        Host host = event.getNode();
        logger.trace("Connection to host {} failed, cause: {}", host, event.getCause());
        if(partialView.contains(host)) {
            setupTimer(new ReconnectTimeout(host), reconnectTimeout);
        }
    }

    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        Host neighbour = event.getNode();
        logger.trace("Host (out) {} is up", neighbour);
        if(partialView.contains(neighbour)) {
            logger.debug("Trying sync from neighbour {} up", neighbour);
            startOutgoingSync(neighbour, UUID.randomUUID());
        }
    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        logger.trace("Host (in) {} is up", event.getNode());
    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        logger.trace("Connection from host {} is down, cause: {}", event.getNode(), event.getCause());

        Host host = event.getNode();

        if (removeFromPendingIncomingSyncs(host)) {
            logger.debug("Removed {} from pendingIncomingSyncs due to in connection down {}", host, pendingIncomingSyncs);
        }

        if (host.equals(incomingSync.getHost())) {
            logger.debug("Removed {} from incomingSync due to in connection down", host);
            tryNextIncomingSync();
        }
    }


    /*--------------------------------- Procedures ---------------------------------------- */

    private void handleGossipMessage(GossipMessage msg, Host from, boolean fromSync) {
        Host sender = msg.getOriginalSender();
        vectorClock.incrementClock(sender);

        try {
            this.fileManager.writeOperationToFile(msg, vectorClock);
        } catch (IOException e) {
            logger.error("Error when writing operation to file", e);
        }

        UUID mid = msg.getMid();
        logger.info("RECEIVED {}", mid);
        triggerNotification(new DeliverNotification(mid, msg.getContent()));
        forwardGossipMessage(msg, from);
    }

    private void forwardGossipMessage(GossipMessage msg, Host from) {
        neighbours.forEach(host -> {
            if (!host.equals(from)) {
                logger.debug("Sent {} to {}", msg, host);
                sendMessage(msg, host);
                this.stats.incrementSentFlood();
            }
        });
    }

    private void startOutgoingSync(Host neighbour, UUID msgId) {
        OutgoingSync os = new OutgoingSync(neighbour, msgId);

        if (!outgoingSyncs.contains(os)) {
            logger.debug("Added {} to outgoingSyncs", neighbour);
            this.outgoingSyncs.add(os);
            UUID mid = UUID.randomUUID();
            SendVectorClockMessage msg = new SendVectorClockMessage(mid);
            sendMessage(msg, neighbour);
            this.stats.incrementSentSendVC();
            logger.debug("Sent {} to {}", msg, neighbour);
        }
    }

    private void tryNextIncomingSync() {
        IncomingSync nextIncomingSync = pendingIncomingSyncs.poll();

        if (nextIncomingSync != null) {
            Host currentPending = nextIncomingSync.getHost();
            UUID mid = nextIncomingSync.getMid();
            incomingSync = nextIncomingSync;
            logger.debug("Added {} to incomingSync (try Next); pendingIncomingSyncs {} ", currentPending, pendingIncomingSyncs);
            VectorClockMessage vectorClockMessage = new VectorClockMessage(mid, new VectorClock(vectorClock.getClock()));
            sendMessage(vectorClockMessage, currentPending, TCPChannel.CONNECTION_IN);
            this.stats.incrementSentVC();
        } else {
            incomingSync = new IncomingSync(null, null);
            logger.debug("Added null to incomingSync (try Next); pendingIncomingSyncs {} ", pendingIncomingSyncs);
        }
    }

    private boolean removeFromPendingIncomingSyncs(Host host) {
        boolean removed = false;
        Iterator<IncomingSync> it = this.pendingIncomingSyncs.iterator();
        while(it.hasNext()) {
            if(it.next().getHost().equals(host)) {
                removed = true;
                it.remove();
            }
        }
        return removed;
    }
}