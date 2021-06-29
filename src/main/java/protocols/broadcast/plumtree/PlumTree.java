package protocols.broadcast.plumtree;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.broadcast.common.BroadcastRequest;
import protocols.broadcast.common.DeliverNotification;
import protocols.broadcast.plumtree.messages.*;
import protocols.broadcast.plumtree.notifications.SendVectorClockNotification;
import protocols.broadcast.plumtree.notifications.VectorClockNotification;
import protocols.broadcast.plumtree.requests.SyncOpsRequest;
import protocols.broadcast.plumtree.requests.VectorClockRequest;
import protocols.broadcast.plumtree.timers.ReconnectTimeout;
import protocols.broadcast.plumtree.timers.IHaveTimeout;
import protocols.broadcast.plumtree.utils.AddressedIHaveMessage;
import protocols.broadcast.plumtree.utils.LazyQueuePolicy;
import protocols.broadcast.plumtree.utils.MessageSource;
import protocols.membership.common.notifications.NeighbourDown;
import protocols.membership.common.notifications.NeighbourUp;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.*;

public class PlumTree extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(PlumTree.class);

    public final static short PROTOCOL_ID = 900;
    public final static String PROTOCOL_NAME = "PlumTree";

    protected int channelId;
    private final Host myself;
    private final static int PORT_MAPPING = 1000;

    private final long timeout1;
    private final long timeout2;
    private final long reconnectTimeout;

    private final Set<Host> partialView;
    private final Set<Host> eager;
    private final Set<Host> lazy;
    private final Queue<Host> pending;
    private Host currentPending;
    private final Queue<GossipMessage> bufferedOps; //Buffer ops received between sending vc to kernel and sending sync ops (and send them after)
    private boolean buffering; //To know if we are between sending vc to kernel and sending ops to neighbour
    private final Map<UUID, Queue<MessageSource>> missing;
    private final Set<UUID> received;
    private final Map<UUID, Long> onGoingTimers;
    private final Queue<AddressedIHaveMessage> lazyQueue;
    private final LazyQueuePolicy policy;


    /*--------------------------------- Initialization ---------------------------------------- */

    public PlumTree(Properties properties, Host myself) throws HandlerRegistrationException, IOException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.myself = myself;

        this.timeout1 = Long.parseLong(properties.getProperty("timeout1", "1000"));
        this.timeout2 = Long.parseLong(properties.getProperty("timeout2", "500"));
        this.reconnectTimeout = Long.parseLong(properties.getProperty("reconnect_timeout", "500"));

        this.partialView = new HashSet<>();
        this.eager = new HashSet<>();
        this.lazy = new HashSet<>();
        this.pending = new LinkedList<>();
        this.bufferedOps = new LinkedList<>();
        this.buffering = false;
        this.missing = new HashMap<>();
        this.received = new HashSet<>();
        this.onGoingTimers = new HashMap<>();
        this.lazyQueue = new LinkedList<>();
        this.policy = HashSet::new;

        String cMetricsInterval = properties.getProperty("bcast_channel_metrics_interval", "10000"); // 10 seconds

        // Create a properties object to setup channel-specific properties. See the
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
        registerTimerHandler(IHaveTimeout.TIMER_ID, this::uponIHaveTimeout);
        registerTimerHandler(ReconnectTimeout.TIMER_ID, this::uponReconnectTimeout);

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(BroadcastRequest.REQUEST_ID, this::uponBroadcast);
        registerRequestHandler(VectorClockRequest.REQUEST_ID, this::uponVectorClock);
        registerRequestHandler(SyncOpsRequest.REQUEST_ID, this::uponSyncOps);

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(NeighbourUp.NOTIFICATION_ID, this::uponNeighbourUp);
        subscribeNotification(NeighbourDown.NOTIFICATION_ID, this::uponNeighbourDown);

        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(channelId, GossipMessage.MSG_ID, GossipMessage.serializer);
        registerMessageSerializer(channelId, PruneMessage.MSG_ID, PruneMessage.serializer);
        registerMessageSerializer(channelId, GraftMessage.MSG_ID, GraftMessage.serializer);
        registerMessageSerializer(channelId, IHaveMessage.MSG_ID, IHaveMessage.serializer);

        registerMessageSerializer(channelId, SendVectorClockMessage.MSG_ID, SendVectorClockMessage.serializer);
        registerMessageSerializer(channelId, VectorClockMessage.MSG_ID, VectorClockMessage.serializer);
        registerMessageSerializer(channelId, SyncOpsMessage.MSG_ID, SyncOpsMessage.serializer);

        /*---------------------- Register Message Handlers -------------------------- */
        registerMessageHandler(channelId, GossipMessage.MSG_ID, this::uponReceiveGossip, this::onMessageFailed);
        registerMessageHandler(channelId, PruneMessage.MSG_ID, this::uponReceivePrune, this::onMessageFailed);
        registerMessageHandler(channelId, GraftMessage.MSG_ID, this::uponReceiveGraft, this::onMessageFailed);
        registerMessageHandler(channelId, IHaveMessage.MSG_ID, this::uponReceiveIHave, this::onMessageFailed);

        registerMessageHandler(channelId, SendVectorClockMessage.MSG_ID, this::uponReceiveSendVectorClock, this::onMessageFailed);
        registerMessageHandler(channelId, VectorClockMessage.MSG_ID, this::uponReceiveVectorClock, this::onMessageFailed);
        registerMessageHandler(channelId, SyncOpsMessage.MSG_ID, this::uponReceiveSyncOps, this::onMessageFailed);

        /*-------------------- Register Channel Event ------------------------------- */
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
    }

    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {

    }

    /*--------------------------------- Messages ---------------------------------------- */

    private void uponReceiveGossip(GossipMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg.getMid(), from);
        UUID mid = msg.getMid();
        if (!received.contains(mid)) {
            triggerNotification(new DeliverNotification(mid, from, msg.getContent(), false));
            handleGossipMessage(msg, msg.getRound() + 1, from);
        } else {
            logger.info("{} was duplicated msg from {}", mid, from);
            if (eager.remove(from)) {
                logger.info("Removed {} from eager due to duplicate {}", from, eager);

                if (lazy.add(from)) {
                    logger.info("Added {} to lazy due to duplicate {}", from, lazy);
                }

                logger.info("Sent PruneMessage to {}", from);
                sendMessage(new PruneMessage(), from);
            }
        }
    }

    private void uponReceivePrune(PruneMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
        if (eager.remove(from)) {
            logger.info("Removed {} from eager due to prune {}", from, eager);

            if (lazy.add(from)) {
                logger.info("Added {} to lazy due to prune {}", from, lazy);
            }
        }
    }

    private void uponReceiveGraft(GraftMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
        startSynchronization(from, false);
    }

    private void uponReceiveIHave(IHaveMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}", msg, from);
        handleAnnouncement(msg.getMid(), from, msg.getRound());
    }

    private void uponReceiveVectorClock(VectorClockMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
        if(!from.equals(currentPending))
            return;
        this.buffering = true;
        triggerNotification(new VectorClockNotification(msg.getSender(), msg.getVectorClock()));
    }

    private void uponReceiveSendVectorClock(SendVectorClockMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
        triggerNotification(new SendVectorClockNotification(msg.getSender()));
    }

    private void uponReceiveSyncOps(SyncOpsMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);

        Iterator<byte[]> opIt = msg.getOperations().iterator();
        Iterator<byte[]> idIt = msg.getIds().iterator();

        while (opIt.hasNext() && idIt.hasNext()) {
            byte[] serOp = opIt.next();
            byte[] serId = idIt.next();
            UUID mid = deserializeId(serId);

            if (!received.contains(mid)) {
                triggerNotification(new DeliverNotification(mid, from, serOp, true));
                logger.info("Propagating sync op {} to {}", mid, eager);
                handleGossipMessage(new GossipMessage(mid, from, 0, serOp), 0, from);
            }
        }
    }

    private void onMessageFailed(ProtoMessage protoMessage, Host host, short destProto, Throwable reason, int channel) {
        logger.warn("Message failed to " + host + ", " + protoMessage + ": " + reason.getMessage());
    }

    /*--------------------------------- Timers ---------------------------------------- */

    private void uponIHaveTimeout(IHaveTimeout timeout, long timerId) {
        UUID mid = timeout.getMid();
        if (!received.contains(mid)) {
            MessageSource msgSrc = missing.get(mid).poll();
            if (msgSrc != null) {
                long tid = setupTimer(timeout, timeout2);
                onGoingTimers.put(mid, tid);
                Host neighbour = msgSrc.peer;
                if (partialView.contains(neighbour) && !neighbour.equals(currentPending) && !pending.contains(neighbour)) {
                    logger.info("Sent GraftMessage for {} to {}", mid, neighbour);
                    sendMessage(new GraftMessage(mid, msgSrc.round), neighbour);
                }
                logger.info("Try sync with {} for timeout {}", neighbour, mid);
                startSynchronization(neighbour, false);

            }
        }
    }

    private void uponReconnectTimeout(ReconnectTimeout timeout, long timerId) {
        Host neighbour = timeout.getHost();
        if(partialView.contains(neighbour)) {
            logger.info("Reconnecting with {}", neighbour);
            openConnection(neighbour);
        } else {
            logger.info("Not reconnecting because {} is down", neighbour);
        }
    }


    /*--------------------------------- Requests ---------------------------------------- */

    private void uponBroadcast(BroadcastRequest request, short sourceProto) {
        UUID mid = request.getMsgId();
        Host sender = request.getSender();
        byte[] content = request.getMsg();
        triggerNotification(new DeliverNotification(mid, sender, content, false));
        logger.info("Propagating my {} to {}", mid, eager);
        GossipMessage msg = new GossipMessage(mid, sender, 0, content);
        handleGossipMessage(msg, 0, sender);
    }

    private void uponVectorClock(VectorClockRequest request, short sourceProto) {
        Host neighbour = request.getTo();
        VectorClockMessage msg = new VectorClockMessage(request.getMsgId(), request.getSender(), request.getVectorClock());
        sendMessage(msg, neighbour, TCPChannel.CONNECTION_IN);
        logger.info("Sent {} to {}", msg, neighbour);
    }

    private void uponSyncOps(SyncOpsRequest request, short sourceProto) {
        Host neighbour = request.getTo();
        if(!neighbour.equals(currentPending))
            return;

        SyncOpsMessage msg = new SyncOpsMessage(request.getMsgId(), request.getIds(), request.getOperations());
        sendMessage(msg, neighbour);
        logger.info("Sent {} to {}", msg, neighbour);
        handleBufferedOperations(neighbour);
        addPendingToEager();
    }


    /*--------------------------------- Notifications ---------------------------------------- */

    private void uponNeighbourUp(NeighbourUp notification, short sourceProto) {
        Host tmp = notification.getNeighbour();
        Host neighbour = new Host(tmp.getAddress(),tmp.getPort() + PORT_MAPPING);

        if (partialView.add(neighbour)) {
            logger.info("Added {} to partial view due to up {}", neighbour, partialView);
        }

        openConnection(neighbour);
    }

    private void uponNeighbourDown(NeighbourDown notification, short sourceProto) {
        Host tmp = notification.getNeighbour();
        Host neighbour = new Host(tmp.getAddress(),tmp.getPort() + PORT_MAPPING);

        if (partialView.remove(neighbour)) {
            logger.info("Removed {} from partial view due to down {}", neighbour, partialView);
        }

        if (eager.remove(neighbour)) {
            logger.info("Removed {} from eager due to down {}", neighbour, eager);
        }

        if (lazy.remove(neighbour)) {
            logger.info("Removed {} from lazy due to down {}", neighbour, lazy);
        }

        if (pending.remove(neighbour)) {
            logger.info("Removed {} from pending due to down {}", neighbour, pending);
        }

        MessageSource msgSrc = new MessageSource(neighbour, 0);
        for (Queue<MessageSource> iHaves : missing.values()) {
            iHaves.remove(msgSrc);
        }

        if (neighbour.equals(currentPending)) {
            logger.info("Removed {} from current pending due to down", neighbour);
            tryNextSync();
        }
        closeConnection(neighbour);
    }


    /*--------------------------------- Procedures ---------------------------------------- */

    private void startSynchronization(Host neighbour, boolean neighUp) {
        if (neighUp || (lazy.contains(neighbour) && !neighbour.equals(currentPending) && !pending.contains(neighbour))) {
            if (currentPending == null) {
                currentPending = neighbour;
                logger.info("{} is my currentPending start", neighbour);
                requestVectorClock(currentPending);
            } else {
                pending.add(neighbour);
                logger.info("Added {} to pending {}", neighbour, pending);
            }
        }
    }

    private void addPendingToEager() {
        if (eager.add(currentPending)) {
            logger.info("Added {} to eager {} : pending list {}", currentPending, eager, pending);
        }

        if (lazy.remove(currentPending)) {
            logger.info("Removed {} from lazy due to sync {}", currentPending, lazy);
        }
        tryNextSync();
    }

    private void tryNextSync() {
        currentPending = pending.poll();
        if (currentPending != null) {
            logger.info("{} is my currentPending try", currentPending);
            requestVectorClock(currentPending);
        }
    }

    private void requestVectorClock(Host neighbour) {
        SendVectorClockMessage msg = new SendVectorClockMessage(UUID.randomUUID(), myself);
        sendMessage(msg, neighbour);
        logger.info("Sent {} to {}", msg, neighbour);
    }

    private void handleGossipMessage(GossipMessage msg, int round, Host from) {
        if(buffering)
            this.bufferedOps.add(msg);

        UUID mid = msg.getMid();
        received.add(mid);

        Long tid;
        if ((tid = onGoingTimers.remove(mid)) != null) {
            cancelTimer(tid);
        }

        eagerPush(msg, round, from);
        lazyPush(msg, round, from);
    }

    private void handleAnnouncement(UUID mid, Host from, int round) {
        if (!received.contains(mid)) {
            if (!onGoingTimers.containsKey(mid)) {
                long tid = setupTimer(new IHaveTimeout(mid), timeout1);
                onGoingTimers.put(mid, tid);
            }
            missing.computeIfAbsent(mid, v -> new LinkedList<>()).add(new MessageSource(from, round));
        }
    }

    private void handleBufferedOperations(Host neighbour) {
        this.buffering = false;
        GossipMessage msg;
        while((msg = bufferedOps.poll()) != null) {
            if(!msg.getSender().equals(neighbour)) {
                sendMessage(msg, neighbour);
                logger.info("Sent buffered {} to {}", msg, neighbour);
            }
        }
    }

    private void eagerPush(GossipMessage msg, int round, Host from) {
        msg.setRound(round);
        for (Host peer : eager) {
            if (!peer.equals(from)) {
                sendMessage(msg, peer);
                logger.info("Forward {} received from {} to {}", msg.getMid(), from, peer);
            }
        }
    }

    private void lazyPush(GossipMessage msg, int round, Host from) {
        for (Host peer : lazy) {
            if (!peer.equals(from)) {
                lazyQueue.add(new AddressedIHaveMessage(new IHaveMessage(msg.getMid(), round), peer));
            }
        }
        dispatch();
    }

    private void dispatch() {
        Set<AddressedIHaveMessage> announcements = policy.apply(lazyQueue);
        for (AddressedIHaveMessage msg : announcements) {
            logger.debug("Sent {} to {}", msg.msg, msg.to);
            sendMessage(msg.msg, msg.to);
        }
        lazyQueue.removeAll(announcements);
    }

    private UUID deserializeId(byte[] msg) {
        ByteBuf buf = Unpooled.buffer().writeBytes(msg);
        return new UUID(buf.readLong(), buf.readLong());
    }

    /* --------------------------------- Channel Events ---------------------------- */

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        Host host = event.getNode();
        logger.info("Host {} is down, cause: {}", host, event.getCause());

        if (eager.remove(host)) {
            logger.info("Removed {} from eager due to plumtree down {}", host, eager);
        }

        if (lazy.remove(host)) {
            logger.info("Removed {} from lazy due to plumtree down {}", host, lazy);
        }

        if (pending.remove(host)) {
            logger.info("Removed {} from pending due to plumtree down {}", host, pending);
        }

        if (host.equals(currentPending)) {
            logger.info("Removed {} from current pending due to plumtree down", host);
            tryNextSync();
        }
        setupTimer(new ReconnectTimeout(host), reconnectTimeout);
    }

    private void uponOutConnectionFailed(OutConnectionFailed event, int channelId) {
        Host host = event.getNode();
        logger.info("Connection to host {} failed, cause: {}", host, event.getCause());
        setupTimer(new ReconnectTimeout(host), reconnectTimeout);
    }

    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        Host neighbour = event.getNode();
        logger.trace("Host (out) {} is up", neighbour);
        if(partialView.contains(neighbour)) {
            logger.info("Trying sync from neighbour {} up", neighbour);
            startSynchronization(neighbour, true);
        }
    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        logger.trace("Host (in) {} is up", event.getNode());
    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        logger.trace("Connection from host {} is down, cause: {}", event.getNode(), event.getCause());
    }

}