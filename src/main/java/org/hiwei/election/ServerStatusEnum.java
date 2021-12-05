package org.hiwei.election;

public enum ServerStatusEnum {
    LOOKING(1,"LOOKING"),
    LEADER(2,"FOLLOWER"),
    FOLLOWER(3,"FOLLOWER");

    private int code;
    private String status;

    ServerStatusEnum(int code, String status){
        this.code = code;
        this.status = status;
    }

    public int getCode() {
        return code;
    }

    public String getStatus() {
        return status;
    }
}
