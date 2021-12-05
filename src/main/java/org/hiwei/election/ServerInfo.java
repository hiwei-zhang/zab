package org.hiwei.election;

import java.util.concurrent.atomic.AtomicLong;

public class ServerInfo {
    public static ServerStatusEnum status = ServerStatusEnum.LOOKING;

    public static Long zxid = 0L;

    public static Vote currentVote = null;

    public static AtomicLong logicalclock = new AtomicLong(0);

    public static boolean isRunning = true;

    public static int peerSize = 0;
}
