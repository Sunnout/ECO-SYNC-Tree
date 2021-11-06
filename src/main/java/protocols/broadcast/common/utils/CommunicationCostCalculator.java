package protocols.broadcast.common.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.channel.tcp.events.ChannelMetrics;

public abstract class CommunicationCostCalculator extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(CommunicationCostCalculator.class);

    private long lastTotalReceived;
    private long lastTotalSent;

    public CommunicationCostCalculator(String protoName, short protoId) {
        super(protoName, protoId);
        this.lastTotalReceived = 0;
        this.lastTotalSent = 0;
    }

    /**
     * If we passed a value > 0 in the METRICS_INTERVAL_KEY property of the channel, this event will be triggered
     * periodically by the channel. "getInConnections" and "getOutConnections" returns the currently established
     * connection to/from me. "getOldInConnections" and "getOldOutConnections" returns connections that have already
     * been closed.
     */
    protected void uponChannelMetrics(ChannelMetrics event, int channelId) {
        StringBuilder sb = new StringBuilder("Channel Metrics: ");
        long totalBytesSent = 0;
        long totalBytesReceived = 0;

        for(ChannelMetrics.ConnectionMetrics c: event.getOutConnections()){
            totalBytesSent += c.getSentAppBytes();
            totalBytesReceived += c.getReceivedAppBytes();
        }

        for(ChannelMetrics.ConnectionMetrics c: event.getOldOutConnections()){
            totalBytesSent += c.getSentAppBytes();
            totalBytesReceived += c.getReceivedAppBytes();
        }

        for(ChannelMetrics.ConnectionMetrics c: event.getInConnections()){
            totalBytesSent += c.getSentAppBytes();
            totalBytesReceived += c.getReceivedAppBytes();
        }

        for(ChannelMetrics.ConnectionMetrics c: event.getOldInConnections()){
            totalBytesSent += c.getSentAppBytes();
            totalBytesReceived += c.getReceivedAppBytes();
        }

        long bytesReceivedSinceLast = totalBytesReceived - lastTotalReceived;
        long bytesSentSinceLast = totalBytesSent - lastTotalSent;
        lastTotalSent = totalBytesSent;
        lastTotalReceived = totalBytesReceived;

        sb.append(String.format("BytesSent=%s ", bytesSentSinceLast));
        sb.append(String.format("BytesReceived=%s", bytesReceivedSinceLast));
        logger.info(sb);
    }
}
