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
import protocols.broadcast.plumtree.timers.IHaveTimeout;
import protocols.broadcast.plumtree.utils.AddressedIHaveMessage;
import protocols.broadcast.plumtree.utils.LazyQueuePolicy;
import protocols.broadcast.plumtree.utils.MessageSource;
import protocols.membership.common.notifications.ChannelCreated;
import protocols.membership.common.notifications.NeighbourDown;
import protocols.membership.common.notifications.NeighbourUp;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.*;

public class PlumTree extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(PlumTree.class);

    public final static short PROTOCOL_ID = 900;
    public final static String PROTOCOL_NAME = "PlumTree";

    private final int space;

    private final long timeout1;
    private final long timeout2;

    private final Host myself;

    private final Set<Host> eager;
    private final Set<Host> lazy;

    private final Queue<Host> pending;
    private Host currentPending;
    private Queue<GossipMessage> bufferedOps; //Buffer ops received between sending vc to kernel and sending sync ops (and send them after)
    private boolean buffering; //To know if we are between sending vc to kernel and sending ops to neighbour

    private final Map<UUID, Queue<MessageSource>> missing;
    private final Map<UUID, GossipMessage> received;

    private final Queue<UUID> stored;

    private final Map<UUID, Long> onGoingTimers;
    private final Queue<AddressedIHaveMessage> lazyQueue;

    private final LazyQueuePolicy policy;

    private boolean channelReady;


    /*--------------------------------- Initialization ---------------------------------------- */

    public PlumTree(Properties properties, Host myself) throws HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.myself = myself;

        this.space = Integer.parseInt(properties.getProperty("space", "6000"));
        this.stored = new LinkedList<>();

        this.eager = new HashSet<>();
        this.lazy = new HashSet<>();

        this.pending = new LinkedList<>();
        this.bufferedOps = new LinkedList<>();
        this.buffering = false;

        this.missing = new HashMap<>();
        this.received = new HashMap<>();
        this.onGoingTimers = new HashMap<>();
        this.lazyQueue = new LinkedList<>();

        this.policy = HashSet::new;

        this.timeout1 = Long.parseLong(properties.getProperty("timeout1", "1000"));
        this.timeout2 = Long.parseLong(properties.getProperty("timeout2", "500"));

        this.channelReady = false;

        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(IHaveTimeout.TIMER_ID, this::uponIHaveTimeout);

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(BroadcastRequest.REQUEST_ID, this::uponBroadcast);
        registerRequestHandler(VectorClockRequest.REQUEST_ID, this::uponVectorClock);
        registerRequestHandler(SyncOpsRequest.REQUEST_ID, this::uponSyncOps);

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(NeighbourUp.NOTIFICATION_ID, this::uponNeighbourUp);
        subscribeNotification(NeighbourDown.NOTIFICATION_ID, this::uponNeighbourDown);
        subscribeNotification(ChannelCreated.NOTIFICATION_ID, this::uponChannelCreated);
    }

    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {

    }

    private void uponChannelCreated(ChannelCreated notification, short sourceProto) {
        int channelId = notification.getChannelId();
        registerSharedChannel(channelId);

        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(channelId, GossipMessage.MSG_ID, GossipMessage.serializer);
        registerMessageSerializer(channelId, PruneMessage.MSG_ID, PruneMessage.serializer);
        registerMessageSerializer(channelId, GraftMessage.MSG_ID, GraftMessage.serializer);
        registerMessageSerializer(channelId, IHaveMessage.MSG_ID, IHaveMessage.serializer);

        registerMessageSerializer(channelId, SendVectorClockMessage.MSG_ID, SendVectorClockMessage.serializer);
        registerMessageSerializer(channelId, VectorClockMessage.MSG_ID, VectorClockMessage.serializer);
        registerMessageSerializer(channelId, SyncOpsMessage.MSG_ID, SyncOpsMessage.serializer);

        try {

            /*---------------------- Register Message Handlers -------------------------- */
            registerMessageHandler(channelId, GossipMessage.MSG_ID, this::uponReceiveGossip);
            registerMessageHandler(channelId, PruneMessage.MSG_ID, this::uponReceivePrune);
            registerMessageHandler(channelId, GraftMessage.MSG_ID, this::uponReceiveGraft);
            registerMessageHandler(channelId, IHaveMessage.MSG_ID, this::uponReceiveIHave);

            registerMessageHandler(channelId, SendVectorClockMessage.MSG_ID, this::uponReceiveSendVectorClock);
            registerMessageHandler(channelId, VectorClockMessage.MSG_ID, this::uponReceiveVectorClock);
            registerMessageHandler(channelId, SyncOpsMessage.MSG_ID, this::uponReceiveSyncOps);

        } catch (HandlerRegistrationException e) {
            logger.error("Error registering message handler: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }

        this.channelReady = true;
    }


    /*--------------------------------- Messages ---------------------------------------- */

    private void uponReceiveGossip(GossipMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg.getMid(), from);
        UUID mid = msg.getMid();
        if (!received.containsKey(mid)) {
            int round = msg.getRound();

            triggerNotification(new DeliverNotification(mid, from, msg.getContent(), false));
            handleGossipMessage(msg, round + 1, from);

            /*
            //TODO: estava receber mensagens de alguém que não está em nenhum lado quando tinha !lazy.contains(from)??

            if (from.equals(currentPending) || eager.contains(from) || pending.contains(from)) {
                triggerNotification(new DeliverNotification(mid, from, msg.getContent(), false));
                handleGossipMessage(msg, round + 1, from);
                //Se eu não adicionar aqui ao eager e remover do lazy então no início não se forma a árvore otimizada?
                //Ou forma pq o hyparview é simétrico e se alguém me mandar uma msg no início é pq tb me tem na eager?
            } else {
                logger.info("{} was received from lazy {}", mid, from);
                handleAnnouncement(mid, from, round);
                //TODO: talvez ligar assim seja mau pq podes fazer sync logo após prune
//                startSynchronization(from, true);
            }*/

        } else {
            logger.info("{} was duplicated msg from {}", mid, from);

            if (eager.remove(from)) {
                logger.info("Removed {} from eager due to duplicate {}", from, eager);
                logger.info("Sent PruneMessage to {}", from);
                sendMessage(new PruneMessage(), from);
            }

            if (lazy.add(from)) {
                logger.info("Added {} to lazy due to duplicate {}", from, lazy);
            }
        }
    }

    private void uponReceivePrune(PruneMessage msg, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", msg, from);
        if (eager.remove(from)) {
            logger.info("Removed {} from eager due to prune {}", from, eager);
        }

        if (lazy.add(from)) {
            logger.info("Added {} to lazy due to prune {}", from, lazy);
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

            //These are not delivered if duplicated due to out of order
            if (!received.containsKey(mid)) {
                //TODO: check se o from aqui é o que envia ou o sender original
                GossipMessage gossipMessage = new GossipMessage(mid, from, 0, serOp);
                triggerNotification(new DeliverNotification(mid, from, serOp, true));
                logger.info("Propagating sync op {} to {}", mid, eager);
                handleGossipMessage(gossipMessage, 0, from);
            }
        }
    }

    /*--------------------------------- Timers ---------------------------------------- */

    private void uponIHaveTimeout(IHaveTimeout timeout, long timerId) {
        UUID mid = timeout.getMid();
        if (!received.containsKey(mid)) {
            MessageSource msgSrc = missing.get(mid).poll();
            if (msgSrc != null) {
                long tid = setupTimer(timeout, timeout2);
                onGoingTimers.put(mid, tid);
                Host neighbour = msgSrc.peer;
                startSynchronization(neighbour, false);
                logger.info("Sent GraftMessage for {} to {}", mid, neighbour);
                sendMessage(new GraftMessage(mid, msgSrc.round), neighbour);
            }
        }
    }


    /*--------------------------------- Requests ---------------------------------------- */

    private void uponBroadcast(BroadcastRequest request, short sourceProto) {
        if (!channelReady)
            return;

        UUID mid = request.getMsgId();
        Host sender = request.getSender();
        byte[] content = request.getMsg();
        triggerNotification(new DeliverNotification(mid, sender, content, false));
        logger.info("Propagating my {} to {}", mid, eager);
        GossipMessage msg = new GossipMessage(mid, sender, 0, content);
        handleGossipMessage(msg, 0, sender);
    }

    private void uponVectorClock(VectorClockRequest request, short sourceProto) {
        if (!channelReady)
            return;

        Host neighbour = request.getTo();
        VectorClockMessage msg = new VectorClockMessage(request.getMsgId(), request.getSender(), request.getVectorClock());
        sendMessage(msg, neighbour);
        logger.info("Sent {} to {}", msg, neighbour);
    }

    private void uponSyncOps(SyncOpsRequest request, short sourceProto) {
        if (!channelReady)
            return;

        Host neighbour = request.getTo();
        SyncOpsMessage msg = new SyncOpsMessage(request.getMsgId(), request.getIds(), request.getOperations());
        sendMessage(msg, neighbour);
        logger.info("Sent {} to {}", msg, neighbour);
        handleBufferedOperations(neighbour);
        addPendingToEager();
    }


    /*--------------------------------- Notifications ---------------------------------------- */

    private void uponNeighbourUp(NeighbourUp notification, short sourceProto) {
        Host neighbour = notification.getNeighbour();
        logger.info("Trying sync from neighbour {} up", neighbour);
        startSynchronization(neighbour, false);
    }

    private void uponNeighbourDown(NeighbourDown notification, short sourceProto) {
        Host neighbour = notification.getNeighbour();
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
            tryNextSync();
        }
        closeConnection(neighbour);
    }


    /*--------------------------------- Procedures ---------------------------------------- */

    private void startSynchronization(Host neighbour, boolean fromGossip) {
        if (!neighbour.equals(currentPending) && !eager.contains(neighbour) && !pending.contains(neighbour)) {
            if (fromGossip) {
                logger.info("Sync with {} was from gossip", neighbour);
                openConnection(neighbour);
            }
            if (currentPending == null) {
                currentPending = neighbour;
                logger.info("{} is my currentPending", neighbour);
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

        //Passei a remover da lazy só no fim em vez de remover mal começo sync
        if (lazy.remove(currentPending)) {
            logger.info("Removed {} from lazy due to sync {}", currentPending, lazy);
        }

        tryNextSync();
    }

    private void tryNextSync() {
        currentPending = pending.poll();
        if (currentPending != null) {
            logger.info("{} is my currentPending", currentPending);
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
        received.put(mid, msg);
        stored.add(mid);
        if (stored.size() > space) {
            UUID toRemove = stored.poll();
            received.put(toRemove, null);
        }

        Long tid;
        if ((tid = onGoingTimers.remove(mid)) != null) {
            cancelTimer(tid);
        }

        eagerPush(msg, round, from);
        lazyPush(msg, round, from);
    }

    private void handleAnnouncement(UUID mid, Host from, int round) {
        if (!received.containsKey(mid)) {
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
            sendMessage(msg, neighbour);
            logger.info("Sent buffered {} to {}", msg, neighbour);
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

//        if(synched.getOrDefault(currentPending, false)) {
//            sendMessage(msg, currentPending);
//            logger.info("Forward {} received from {} to {} because it is already synched", msg.getMid(), from, currentPending);
//        }
    }

    private void lazyPush(GossipMessage msg, int round, Host from) {
        for (Host peer : lazy) {
            if (!peer.equals(from)) { //TODO: deu null pointer aqui??
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

}
