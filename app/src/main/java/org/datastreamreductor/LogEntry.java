package org.datastreamreductor;

import java.io.Serializable;

/**
 * Created by ext_lmarzo on 9/5/17.
 */
public class LogEntry implements Serializable{

    private String hash;
    private String message;

    public LogEntry(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return message;
    }
}
