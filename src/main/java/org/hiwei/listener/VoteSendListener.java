package org.hiwei.listener;

import org.hiwei.election.ServerInfo;
import org.hiwei.election.ZkSocket;
import org.hiwei.election.Vote;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.*;

public class VoteSendListener extends Thread{
    private BlockingQueue<Vote> sendQueue;

    private Map<Long,ZkSocket> socketMap ;

    private Socket socket;

    private Long sid;

    private ThreadPoolExecutor pool = new ThreadPoolExecutor(10,10,60,TimeUnit.SECONDS
            ,new LinkedBlockingQueue<>());

    public VoteSendListener(BlockingQueue<Vote> sendQueue, Map<Long, ZkSocket> socketMap, Long sid){
        this.sendQueue = sendQueue;
        this.socketMap = socketMap;
        this.sid = sid;
    }

    @Override
    public void run(){
        while(ServerInfo.isRunning){
            try {
                Vote vote = sendQueue.poll(10, TimeUnit.SECONDS);
                if(vote!=null){
                    ByteBuffer byteBuffer = buildMsg(vote.getId(), vote.getZxid(),vote.getEpoch(),vote.getState());
                    socketMap.forEach((sid,zkSocket)->{
                        Runnable task = new Runnable(){
                            @Override
                            public void run() {
                                DataOutputStream don = zkSocket.getDon();
                                try {
                                    send(byteBuffer,don);
                                    don.flush();
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                        };
                        pool.execute(task);
                    });
                }
            } catch (InterruptedException e) {

            }
        }
    }
    synchronized void send(ByteBuffer b, DataOutputStream dout) throws IOException {
        byte[] msgBytes = new byte[b.capacity()];
        try {
            b.position(0);
            b.get(msgBytes);
        } catch (BufferUnderflowException be) {
            return;
        }
        dout.writeInt(b.capacity());
        dout.write(b.array());
        dout.flush();
    }
    static ByteBuffer buildMsg(long leader, long zxid,long epoch,int state) {
        byte[] requestBytes = new byte[28];
        ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);
        /*
         * Building notification packet to send
         */
        requestBuffer.clear();
        requestBuffer.putLong(leader);
        requestBuffer.putLong(zxid);
        requestBuffer.putLong(epoch);
        requestBuffer.putInt(state);

        return requestBuffer;
    }
}
