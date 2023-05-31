package org.hiwei.election;

import java.util.concurrent.atomic.AtomicLong;

public class ServerInfo {
    public static ServerStatusEnum status = ServerStatusEnum.LOOKING;

    /**
     * 最大事务id
     */
    public static Long zxid = 0L;

    public static Vote currentVote = null;

    /**
     * 逻辑时钟
     */
    public static AtomicLong logicalClock = new AtomicLong(0);

    public static boolean isRunning = true;

    public static int peerSize = 0;
}
