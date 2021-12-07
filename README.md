本工程只实现了选举功能，根据zookeeper选举流程实现，zookeeper用到了多级队列，此处只用到了一级，结构更加简单，便于深入理解zookeeper选举机制。

项目启动：

QuorumPeerMain.main方法启动。
![在这里插入图片描述](https://img-blog.csdnimg.cn/cf746230b2b7458bb14f4dbda3a43160.png?x-oss-process=image/watermark,type_d3F5LXplbmhlaQ,shadow_50,text_Q1NETiBA5Lmm5Lit5pWF5LqL,size_20,color_FFFFFF,t_70,g_se,x_16)
建立三个启动项。zoo.cfg换为自己地址即可。在resources目录下已经建立三个zoo.cfg文件。这里只配置用于选举的端口地址。myid和zookeeper的myid用处一样。
依次启动三个应用，就可以在控制台看到选举信息。
# zookeeper集群
![在这里插入图片描述](https://img-blog.csdnimg.cn/ab7a1d5d1a2144ada83a89e19d6eed95.png?x-oss-process=image/watermark,type_d3F5LXplbmhlaQ,shadow_50,text_Q1NETiBA5Lmm5Lit5pWF5LqL,size_20,color_FFFFFF,t_70,g_se,x_16)
zookeeper集群中，observer是不参与选举的，其主要作用是分担大量读的压力。follower参与选举，不处理事务请求，当事务请求落到follower或observer上时，这些节点会将事务请求转发到leader节点。leader节点挂了，所有的follower会重新进行选举。
# zookeeper选举
## 选举要素
选举的三个要素：
1. 每个zookeeper节点有三个状态：
* LOOKING：游离态（没入党），节点的初始状态，不在集群中，等待参与选举。
* FOLLOWER：已入党，参入过选举，知道leader是谁。
* LEADER：党的领导人。

2. 选举投票Vote信息：
* id：选投为leader的id
* zxid：机器的最大事务id
* epoch：选举周期
* state：投票机器的状态（LOOKING/FOLLOWER/LEADER）

3. 机器节点间通信：
每个机器都有一个专用于选举的端口。

## 选举流程
以上三个要素都有了，下面看是怎么选举的，以3个节点为例：

* 服务启动都要与其它节点建立通信：
![在这里插入图片描述](https://img-blog.csdnimg.cn/a42004cd952a47bcb6ba46facfc62aa5.png?x-oss-process=image/watermark,type_d3F5LXplbmhlaQ,shadow_50,text_Q1NETiBA5Lmm5Lit5pWF5LqL,size_13,color_FFFFFF,t_70,g_se,x_16)
后面发送选票和接收选票都用这个socket连接进行。socket连接是全双工通信，所以每两个节点间，只需要建立一条socket连接即可。
1. 第一轮投票
在服务启动参与选举时，每个节点先投自己一票：
![在这里插入图片描述](https://img-blog.csdnimg.cn/f7e92ea857e842d6831d4dd5a7df74af.png?x-oss-process=image/watermark,type_d3F5LXplbmhlaQ,shadow_50,text_Q1NETiBA5Lmm5Lit5pWF5LqL,size_20,color_FFFFFF,t_70,g_se,x_16)
2. 第二轮投票
因为第一轮投票，每个节点都是投自己一票，所以无法满足票数过半的要求，所以要进行第二轮投票，这时，每个节点会将第一轮pk获胜的选票投出。
![在这里插入图片描述](https://img-blog.csdnimg.cn/a197b309861442218b7f5cae17a56bfe.png?x-oss-process=image/watermark,type_d3F5LXplbmhlaQ,shadow_50,text_Q1NETiBA5Lmm5Lit5pWF5LqL,size_20,color_FFFFFF,t_70,g_se,x_16)
3. 选举结果
根据第二轮投票，必定会出现票数过半的票。最终选举节点2为leader：
![在这里插入图片描述](https://img-blog.csdnimg.cn/3e779629452d40abb3296c2b19a7edd4.png)
**后面加入的机器，如何选举leader呢？**
当leader已经选举完成后，集群中已经有leader节点了，所以再有机器加入也不用重新选举了。新加入的机器，会投自己一票给其它节点，票中有state表明自己为LOOKING状态。集群中的节点收到选票后，会判断选票是否为LOOKING机器投的。如果是则将已知的leader投出去，这样新加入的机器，就会获得leader节点选票，直接获得leader。
![在这里插入图片描述](https://img-blog.csdnimg.cn/a5203940b0274864b71f2c16db3fc833.png?x-oss-process=image/watermark,type_d3F5LXplbmhlaQ,shadow_50,text_Q1NETiBA5Lmm5Lit5pWF5LqL,size_19,color_FFFFFF,t_70,g_se,x_16)
# 手写zookeeper选举流程
## 整体流程图：
![在这里插入图片描述](https://img-blog.csdnimg.cn/b5a3cf7638304da6bd77ea65c08673d9.png?x-oss-process=image/watermark,type_d3F5LXplbmhlaQ,shadow_50,text_Q1NETiBA5Lmm5Lit5pWF5LqL,size_20,color_FFFFFF,t_70,g_se,x_16)
实现架构：
![在这里插入图片描述](https://img-blog.csdnimg.cn/10e6cb0d6057466693c1127859ae69dd.png?x-oss-process=image/watermark,type_d3F5LXplbmhlaQ,shadow_50,text_Q1NETiBA5Lmm5Lit5pWF5LqL,size_20,color_FFFFFF,t_70,g_se,x_16)
1. 如何保证两个节点间只有一个socket连接？
```java
        private void handleConnection(ZkSocket zkSocket) {
            Long sid = null, protocolVersion = null;
            try {
                Socket sock = zkSocket.getSocket();
                protocolVersion = zkSocket.getDin().readLong();
                if (protocolVersion >= 0) { // this is a server id and not a protocol version
                    sid = protocolVersion;
                }
                //判断对方id是否大于本机id，大于则保存连接。否则关闭连接，并重新主动去连接
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
```
2. leader挂掉，怎么触发选举？
与leader保持心跳连接，当检测到失去连接后，重置本机状态，并发送选票，参与选举。
```java
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
```
