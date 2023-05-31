package org.hiwei.election;

public class Vote {

    private final long id;
    /**
     * 最大事务id
     */
    private final long zxid;

    /**
     * 选票朝代
     */
    private long epoch;

    public boolean isLeader() {
        return leader;
    }

    public void setLeader(boolean leader) {
        this.leader = leader;
    }

    private boolean leader;

    private final int state;

    public Vote(long id, long zxid, long epoch,int state) {
        this.id = id;
        this.zxid = zxid;
        this.epoch = epoch;
        this.state = state;
    }

    public long getId() {
        return id;
    }

    public long getZxid() {
        return zxid;
    }

    public long getEpoch() {
        return epoch;
    }

    public int getState() {
        return state;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    @Override
    public String toString() {
        return "Vote{" +
                "id=" + id +
                ", zxid=" + zxid +
                ", epoch=" + epoch +
                ", leader=" + leader +
                ", state=" + state +
                '}';
    }
}
