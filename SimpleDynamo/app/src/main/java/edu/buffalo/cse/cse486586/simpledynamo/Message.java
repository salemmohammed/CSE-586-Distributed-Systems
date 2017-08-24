package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;

/**
 * Created by xiaoxin on 4/8/17.
 */

public class Message implements Serializable{
    public String key;
    public String value;
    public String time_stamp;
    public String type;
    public ArrayList<String[]> dhtChord;
    public LinkedHashMap<String, String> table;

    Message(){
        dhtChord = new ArrayList<String[]>();
        table = new LinkedHashMap<String, String>();
    }
}
