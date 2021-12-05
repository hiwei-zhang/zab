package org.hiwei.listener;

import org.hiwei.election.ServerInfo;
import org.hiwei.election.ServerStatusEnum;
import org.hiwei.election.ZkSocket;
import org.hiwei.election.Vote;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class VoteRecvListener extends Thread{
    private static Logger log = LoggerFactory.getLogger(VoteRecvListener.class);
    private Map<Long, VoteRecvListener> recvListenerMap;
    private BlockingQueue<Vote> recvQueue;
    private BlockingQueue<Vote> sendQueue;

    private Long sid;

    private Map<Long,ZkSocket> socketMap;

    private Long myid;

    public VoteRecvListener(Map<Long,ZkSocket> socketMap, BlockingQueue<Vote> recvQueue,BlockingQueue<Vote> sendQueue
            , Long sid,Long myid,Map<Long, VoteRecvListener> recvListenerMap){
        this.recvQueue = recvQueue;
        this.sendQueue = sendQueue;
        this.sid = sid;
        this.myid = myid;
        this.socketMap = socketMap;
        this.recvListenerMap = recvListenerMap;
    }
    @Override
    public void run(){
        //获取选票
        while(ServerInfo.isRunning){
            ZkSocket zkSocket = socketMap.get(sid);
            if(zkSocket!=null){
                try {
                    DataInputStream din = zkSocket.getDin();
                    int capacity = din.readInt();
                    if(capacity>0){
                        byte[] bytes = new byte[capacity];
                        din.readFully(bytes,0,capacity);
                        recvQueue.offer(parseMsg(bytes));
                    }
                } catch (Exception e) {
                    log.error("connect closed : {}",zkSocket.getSocket());
                    socketMap.remove(sid);
                    recvListenerMap.remove(sid);
                    if(ServerInfo.status!=ServerStatusEnum.LOOKING&&ServerInfo.currentVote.getId()==sid){
                        //发起选举
                        log.info("leader {}失去连接，重新选举......",sid);
                        ServerInfo.status = ServerStatusEnum.LOOKING;
                        ServerInfo.currentVote = null;
                    }
                    break;
                }
            }
        }
    }
    static Vote parseMsg(byte[] bytes) {
        ByteBuffer requestBuffer = ByteBuffer.wrap(bytes);
        /*
         * Building notification packet to send
         */
        long leader = requestBuffer.getLong();
        long zxid = requestBuffer.getLong();
        long epoch = requestBuffer.getLong();
        int state = requestBuffer.getInt();
        Vote vote = new Vote(leader,zxid,epoch,state);
        return vote;
    }
    public static class Message {

        Message(ByteBuffer buffer, long sid) {
            this.buffer = buffer;
            this.sid = sid;
        }

        ByteBuffer buffer;
        long sid;

    }
}
