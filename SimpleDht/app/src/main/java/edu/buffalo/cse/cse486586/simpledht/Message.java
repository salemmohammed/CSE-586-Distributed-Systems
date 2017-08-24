package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentValues;
import android.database.Cursor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;

/**
 * Created by xiaoxin on 4/8/17.
 */

public class Message implements Serializable{
    public String key;
    public String value;
    public String type;
    public ArrayList<String[]> dhtChord;
    public LinkedHashMap<String, String> table;

    Message(){
        dhtChord = new ArrayList<String[]>();
        table = new LinkedHashMap<String, String>();
    }
}
