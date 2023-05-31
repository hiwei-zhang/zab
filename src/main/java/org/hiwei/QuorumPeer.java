package org.hiwei;

import org.hiwei.election.ServerInfo;
import org.hiwei.election.ServerStatusEnum;
import org.hiwei.election.ZkSocket;
import org.hiwei.listener.VoteRecvListener;
import org.hiwei.election.Vote;
import org.hiwei.listener.VoteSendListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class QuorumPeer {
    private static Logger log = LoggerFactory.getLogger(QuorumPeer.class);

    private CountDownLatch countDownLatch;

    private Long myid;

    public Listener listener;

    private BlockingQueue<Vote> sendQueue = new LinkedBlockingDeque<>();

    private BlockingQueue<Vote> recvQueue = new LinkedBlockingDeque<>();

    private final Map<Long, ZkSocket> socketMap = new HashMap<>();

    private final Map<Long, VoteRecvListener> recvListenerMap = new HashMap<>();

    private final Map<Long,String> serverMap = new HashMap<>();

    private final Map<Long, List<Vote>> voteSet = new HashMap<>();

    public QuorumPeer(Properties cfg){
        int serverSize = 0;
        Set<Object> keys = cfg.keySet();
        for(Object key:keys){
            String k = (String)key;
            if(k.startsWith("server.")){
                serverSize++;
            }
            countDownLatch = new CountDownLatch(serverSize);
            ServerInfo.peerSize = serverSize;
        }
        this.listener = new Listener(cfg);

    }

    /**
     * 选举算法
     */
    public void election() {
        List<Vote> voteBucket = new ArrayList<>();
        while(true){
            try {
                Vote recvVote = recvQueue.poll(5, TimeUnit.SECONDS);
                if(ServerInfo.status== ServerStatusEnum.LOOKING&&(socketMap.keySet().size()+1>ServerInfo.peerSize/2)){
                    //开启新的选举
                    log.info("开始选举!");
                    ServerInfo.logicalClock.incrementAndGet();
                    if(ServerInfo.status==ServerStatusEnum.LOOKING){
                        //1.发送选票
                        Vote sVote = new Vote(myid,ServerInfo.zxid,ServerInfo.logicalClock.longValue(),ServerInfo.status.getCode());
                        recvQueue.offer(sVote);
                        sendQueue.offer(sVote);
                        //2.处理选票
                        while(ServerInfo.status==ServerStatusEnum.LOOKING){
                            Vote vote = null;
                            if(recvVote!=null){
                                vote = recvVote;
                                recvVote = null;
                            }else{
                                vote = recvQueue.poll(10, TimeUnit.SECONDS);
                            }
                             if((vote==null||vote.getEpoch()>ServerInfo.logicalClock.intValue())&&!voteBucket.isEmpty()&&voteBucket.size()>1) {
                                 //进行选票pk
                                 Vote lookVote = pkVote(voteBucket);
                                 voteBucket.clear();
                                 if(lookVote.isLeader()){
                                     //选举成功
                                     log.info("选举成功，leader = {}",lookVote.getId());
                                     ServerInfo.currentVote = lookVote;
                                     if(lookVote.getId() == myid){
                                         ServerInfo.status = ServerStatusEnum.LEADER;
                                     }else{
                                         ServerInfo.status = ServerStatusEnum.FOLLOWER;
                                     }
                                     break;
                                 }
                                 //重新发送pk后的选票
                                 ServerInfo.logicalClock.incrementAndGet();
                                 //发送pk选票
                                 Vote lookLeader = new Vote(lookVote.getId(),lookVote.getZxid(), ServerInfo.logicalClock.intValue(),ServerInfo.status.getCode());
                                 log.info("发送pk后的选票:{}",lookLeader);
                                 sendQueue.offer(lookLeader);
                                 recvQueue.offer(lookLeader);
                             };
                             if(vote!=null){
                                 if(vote.getEpoch()==ServerInfo.logicalClock.intValue()){
                                     //放入选票桶
                                     voteBucket.add(vote);
                                 }else{
                                     ServerInfo.logicalClock.set(vote.getEpoch());
                                     //重新发送选票
                                     Vote newVote = new Vote(myid,ServerInfo.zxid,ServerInfo.logicalClock.longValue(),ServerInfo.status.getCode());
                                     log.info("重新发送选票:{}",newVote);
                                     recvQueue.offer(newVote);
                                     sendQueue.offer(newVote);
                                 }
                             }
                        }
                    }
                    ServerInfo.logicalClock.set(0);
                }else if(recvVote!=null){
                    log.info("收到选票：{}",recvVote.toString());
                    if(recvVote.getState()==1){
                        Vote currentVote = ServerInfo.currentVote;
                        Vote vote = new Vote(currentVote.getId(), currentVote.getZxid(), recvVote.getEpoch(),ServerInfo.status.getCode());
                        sendQueue.offer(vote);
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 选票pk，获取获胜节点
     * @param voteBucket
     * @return
     */
    private Vote pkVote(List<Vote> voteBucket) {
        Vote vr =voteBucket.get(0);
        Map<Long,Integer> statistics = new HashMap<>();
        for (Vote vote : voteBucket) {
            if (vote.getZxid() > vr.getZxid()) {
                vr = vote;
            }else if(vote.getId()>vr.getId()){
                vr = vote;
            }
            Integer integer = statistics.get(vote.getId());
            if (integer == null) {
                statistics.put(vote.getId(), 1);
            } else {
                statistics.put(vote.getId(), statistics.get(vote.getId()) + 1);
            }
        }
        for (Map.Entry<Long, Integer> entry : statistics.entrySet()) {
            Long k = entry.getKey();
            Integer v = entry.getValue();
            if (v > ServerInfo.peerSize / 2) {
                for (Vote vt : voteBucket) {
                    if (vt.getId() == k) {
                        vt.setLeader(true);
                        return vt;
                    }
                }
            }
        }
        vr.setLeader(false);
        return vr;
    }

    public class Listener extends Thread {
        private final Properties cfg;

        private final Integer port;
        public Listener(Properties cfg){
            this.cfg = cfg;
            myid = Long.parseLong(cfg.getProperty("myid"));
            this.port = Integer.parseInt(cfg.getProperty("server." + myid).split(":")[1]);
            String localServer = "server." + myid;
            for(Object key:cfg.keySet()){
                String k = (String)key;
                if(k.startsWith("server")){
                    countDownLatch.countDown();
                    if(!localServer.equals(k)){
                        String[] serverId = k.split("\\.");
                        String addr = cfg.getProperty(k);
                        Long sid = Long.parseLong(serverId[1]);
                        serverMap.put(sid,addr);
                        //和这些机器建立连接
                        String[] ap = addr.split(":");
                        ZkSocket socket = connectOn(ap[0], Integer.parseInt(ap[1]));
                        if(myid>sid&&socket!=null){
                            putListener(sid,socket);
                        }
                    }
                }
            }

        }

        private ZkSocket connectOn(String addr, int port){
            ZkSocket socket = null;
            DataOutputStream ow = null;
            BufferedReader is = null;
            try {
                Socket sock = new Socket(addr, port);
                socket = new ZkSocket(sock);
                ow = socket.getDon();
                //发送数据
                ow.writeLong(myid);
                ow.flush();
                log.info("connect to {}:{} success!",addr,port);
                return socket;
            } catch (IOException e) {
                log.error("connect to {}:{} fail!",addr,port);
            }
            return socket;
        }

        @Override
        public void run() {
            try {
                ServerSocket serverSocket = new ServerSocket(port);
                while(ServerInfo.isRunning){
                    Socket sock = serverSocket.accept();
                    ZkSocket zkSocket = new ZkSocket(sock);
                    handleConnection(zkSocket);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        /**
         * 连接处理
         * @param zkSocket
         */
        private void handleConnection(ZkSocket zkSocket) {
            Long sid = null, protocolVersion = null;
            try {
                Socket sock = zkSocket.getSocket();
                protocolVersion = zkSocket.getDin().readLong();
                if (protocolVersion >= 0) {
                    sid = protocolVersion;
                }
                if(sid<myid){
                    closeSocket(sock);
                    if(!socketMap.containsKey(sid)){
                        String address = serverMap.get(sid);
                        String[] ap = address.split(":");
                        ZkSocket socket = connectOn(ap[0], Integer.parseInt(ap[1]));
                        if(socket!=null){
                            putListener(sid,socket);
                        }
                    }
                }else{
                    putListener(sid,zkSocket);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        private void closeSocket(Socket sock) {
            if (sock == null) {
                return;
            }

            try {
                sock.close();
            } catch (IOException ie) {
                log.error("Exception while closing", ie);
            }
        }
    }

    /**
     * 选票发送监听器
     */
    public void startVoteSendListener(){
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        VoteSendListener voteSendListener = new VoteSendListener(sendQueue, socketMap, myid);
        voteSendListener.start();
    }

    public Map<Long, ZkSocket> getSocketMap() {
        return socketMap;
    }

    /**
     * 新增连接
     * @param sid
     * @param zkSocket
     */
    public void putListener(Long sid,ZkSocket zkSocket){
        socketMap.put(sid,zkSocket);
        if(recvListenerMap.get(sid)==null){
            VoteRecvListener voteRecvListener = new VoteRecvListener(socketMap,recvQueue,sendQueue,sid,myid,recvListenerMap);
            voteRecvListener.start();
            recvListenerMap.put(sid, voteRecvListener);
        }
    }
}
