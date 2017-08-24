package edu.buffalo.cse.cse486586.groupmessenger2;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Created by xiaoxin on 3/14/17.
 */

public class Message implements Serializable, Comparable<Message>{
    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public int getOrder() {
        return order;
    }

    public void setOrder(int order) {
        this.order = order;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public boolean isAgreed() {
        return agreed;
    }

    public void setAgreed(boolean agreed) {
        this.agreed = agreed;
    }

    private String content;
    private int order;
    private String port;
    private boolean agreed;

    Message(){
    }

    Message(String content, int order, String port){
        this.content = content;
        this.order = order;
        this.port = port;
    }

    @Override
    public int compareTo(Message another) {
        if(this.order == another.order) return this.port.compareTo(another.port);
        return this.order - another.order;
    }
}
