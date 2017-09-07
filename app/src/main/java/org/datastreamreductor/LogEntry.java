package org.datastreamreductor;

import java.io.Serializable;

/**
 * Created by ext_lmarzo on 9/5/17.
 */
public class LogEntry implements Serializable{

    private int hash;
    private String message;

    public LogEntry(int hash, String message) {
        this.message = message;
        this.hash = hash;
    }

    @Override
    public String toString() {
        return message;
    }

    public int getHash() {
        return hash;
    }

    public String getMessage() {
        return message;
    }

}
