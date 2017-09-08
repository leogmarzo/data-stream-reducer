package org.datastreamreductor;

import java.io.Serializable;

/**
 * Created by ext_lmarzo on 9/5/17.
 */
public class LogEntry implements Serializable{

    private int hash;
    private String stackTrace;
    private String date;

    public LogEntry(int hash, String date, String stackTrace) {
        this.stackTrace = stackTrace;
        this.hash = hash;
        this.date = date;
    }

    @Override
    public String toString() {
        return "hash " + hash + "\n " +
                "date " + date + "\n " +
                "stack trace " + stackTrace;
    }

    public int getHash() {
        return hash;
    }

    public String getStackTrace() {
        return stackTrace;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }
}
