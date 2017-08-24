package edu.buffalo.cse.cse486586.simpledht;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.LinkedHashMap;
import java.util.Map;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static edu.buffalo.cse.cse486586.simpledht.MessageSQLiteHelper.TABLE_MESSAGE;

public class SimpleDhtProvider extends ContentProvider {
    private static final String TAG = SimpleDhtProvider.class.getSimpleName();
    Uri uri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledht.provider");
    private static final int SERVER_PORT = 10000;
    private static final String MANAGER_PORT = "11108";
    private static final String KEY_FIELD = "key", VALUE_FIELD = "value";
    private static final String JOIN = "0", JOIN_AGREE = "1", INSERT = "2", QUERY = "3", QUERYALL = "4", QUERYALL_AGREE = "5", DELETE = "6", DELETEALL = "7";
    private static MessageSQLiteHelper messageSQLiteHelper;
    private static SQLiteDatabase sqLiteDatabaseWriter;
    private static SQLiteDatabase sqLiteDatabaseReader;

    String myPort, myNodeId, mySucc, mySuccNodeId, myPred, myPredNodeId;
    ArrayList<String[]> dhtChord;
    LinkedHashMap<String, String> tempTable = null;
    ArrayList<LinkedHashMap<String, String>> tableList;
    int flag = 0;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub

        sqLiteDatabaseReader = messageSQLiteHelper.getReadableDatabase();

        if (selection.equals("@") || (selection.equals("*") && dhtChord.size() < 2))
            return sqLiteDatabaseWriter.delete(TABLE_MESSAGE, null, null);
        else if(selection.equals("*"))
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, DELETEALL, selection);
        else{
            try {
                String targetPort = getTargetPort(genHash(selection));
                if(targetPort == null) targetPort = myPort;
                if (targetPort.equals(myPort))
                    return sqLiteDatabaseWriter.delete(TABLE_MESSAGE, "key = ?", new String[] {selection});
                else
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, DELETE, targetPort, selection);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

        String key = values.getAsString(KEY_FIELD);
        String value = values.getAsString(VALUE_FIELD);
        sqLiteDatabaseWriter = messageSQLiteHelper.getWritableDatabase();

        try {
            String targetPort = getTargetPort(genHash(key));
            if(targetPort == null) targetPort = myPort;

            if(targetPort.equals(myPort)){
                Cursor cursor = sqLiteDatabaseWriter.rawQuery("select * from " + TABLE_MESSAGE + " where key=?", new String[] {key});

                if (!(cursor.moveToFirst()) || cursor.getCount() == 0) {
                    Long newid = sqLiteDatabaseWriter.insert(MessageSQLiteHelper.TABLE_MESSAGE, null, values);
                    Uri insertUri = ContentUris.withAppendedId(uri, newid);
                    getContext().getContentResolver().notifyChange(insertUri, null);
                    return insertUri;

                }else sqLiteDatabaseWriter.execSQL("UPDATE " + TABLE_MESSAGE + " SET value=? WHERE key =?", new String[] {key, value});

            }else new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, INSERT, targetPort, key, value);

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return null;
    }

    private String getTargetPort(String hash) {
        String targetPort = null;

        for(int i = 1; i < dhtChord.size(); i++){

            if(hash.compareTo(dhtChord.get(i)[1]) <= 0 && hash.compareTo(dhtChord.get(i-1)[1]) > 0)
                targetPort = dhtChord.get(i)[0];

        }
        if(dhtChord.size() > 0 && (hash.compareTo(dhtChord.get(0)[1]) <= 0 || hash.compareTo(dhtChord.get(dhtChord.size()-1)[1]) > 0))
            targetPort = dhtChord.get(0)[0];

        return targetPort;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        messageSQLiteHelper = new MessageSQLiteHelper(getContext());

        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        dhtChord = new ArrayList<String[]>();
        tableList = new ArrayList<LinkedHashMap<String, String>>();

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
        }

        try {
            myNodeId = genHash(portStr);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        myPred = myPort;
        mySucc = myPort;
        myPredNodeId = myNodeId;
        mySuccNodeId = myNodeId;

        if (myPort.equals(MANAGER_PORT)) {
            String[] node = new String[2];
            node[0] = myPort;
            node[1] = myNodeId;
            dhtChord.add(node);

        } else
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, JOIN, myPort);

        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
        sqLiteDatabaseReader = messageSQLiteHelper.getReadableDatabase();
        MatrixCursor cursor = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});

        if (selection.equals("@") || (selection.equals("*") && dhtChord.size() < 2) ){
            return sqLiteDatabaseReader.query(TABLE_MESSAGE, null, null, null, null, null, null);

        }else if(selection.equals("*")){
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, QUERYALL, selection);

            while(flag < dhtChord.size()){}

            for(int i = 0; i < tableList.size(); i ++){

                for(Map.Entry<String, String> entry : tableList.get(i).entrySet())
                    cursor.addRow(new String[]{entry.getKey(), entry.getValue()});
            }
            flag = 0;

            if(cursor.getCount() == 0) return null;

            return cursor;

        }else{

            try {
                String targetPort = getTargetPort(genHash(selection));
                if(targetPort == null) targetPort = myPort;

                if (targetPort.equals(myPort)) {
                    Cursor cur = sqLiteDatabaseReader.rawQuery("select * from " + TABLE_MESSAGE + " where key=?", new String[]{selection});
                    cur.moveToFirst();
                    return cur;

                } else {
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, QUERY, targetPort, selection);

                    while (flag == 0) {}

                    flag = 0;
                    cursor.addRow(new String[]{selection, tempTable.get(selection)});
                    return cursor;
                }
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

        }
        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            Socket socket = null;

            while (true) {

                try {
                    socket = serverSocket.accept();
                    ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                    Message msg = (Message) ois.readObject();

                    if(msg.type.equals(JOIN)) {
                        String portId = genHash(msg.value);
                        String[] node = new String[2];
                        node[0] = msg.key;
                        node[1] = portId;
                        dhtChord.add(node);
                        Collections.sort(dhtChord, new Comparator<String[]>() {
                            @Override
                            public int compare(String[] lhs, String[] rhs) {
                                return lhs[1].compareTo(rhs[1]);
                            }
                        });
                        switchToClient(JOIN_AGREE, myPort);

                    }else if(msg.type.equals(JOIN_AGREE)){
                        dhtChord = msg.dhtChord;

                        if(msg.dhtChord.get(0)[0].equals(myPort)){
                            myPredNodeId = msg.dhtChord.get(msg.dhtChord.size()-1)[1];
                            myPred = msg.dhtChord.get(msg.dhtChord.size()-1)[0];
                            mySuccNodeId = msg.dhtChord.get(1)[1];
                            mySucc = msg.dhtChord.get(1)[0];
                        }

                        for(int i = 1; i < msg.dhtChord.size()-1; i++){
                            if(msg.dhtChord.get(i)[0].equals(myPort)){
                                myPredNodeId = msg.dhtChord.get(i-1)[1];
                                myPred = msg.dhtChord.get(i-1)[0];
                                mySuccNodeId = msg.dhtChord.get(i+1)[1];
                                mySucc = msg.dhtChord.get(i+1)[0];
                            }
                        }

                        if(msg.dhtChord.get(msg.dhtChord.size()-1)[0].equals(myPort)){
                            myPredNodeId = msg.dhtChord.get(msg.dhtChord.size()-2)[1];
                            myPred = msg.dhtChord.get(msg.dhtChord.size()-2)[0];
                            mySuccNodeId = msg.dhtChord.get(0)[1];
                            mySucc = msg.dhtChord.get(0)[0];
                        }

                    }else if(msg.type.equals(INSERT)){

                        ContentValues values = new ContentValues();
                        values.put(KEY_FIELD, msg.key);
                        values.put(VALUE_FIELD, msg.value);
                        insert(uri, values);

                    }else if(msg.type.equals(QUERY)){

                        sqLiteDatabaseReader = messageSQLiteHelper.getReadableDatabase();
                        Cursor cur = sqLiteDatabaseReader.rawQuery("select * from " + TABLE_MESSAGE + " where key=?", new String[] {msg.key});
                        cur.moveToFirst();
                        Message msg2 = new Message();
                        msg2.key = cur.getString(cur.getColumnIndex(KEY_FIELD));
                        msg2.value = cur.getString(cur.getColumnIndex(VALUE_FIELD));
                        msg2.table.put(msg2.key, msg2.value);

                        ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                        oos.writeObject(msg2);
                        oos.flush();

                    }else if(msg.type.equals(QUERYALL)){
                        Cursor cursor = sqLiteDatabaseReader.query(TABLE_MESSAGE, null, null, null, null, null, null);
                        Message msg2 = new Message();

                        while (cursor.moveToNext())
                            msg2.table.put(cursor.getString(cursor.getColumnIndex(KEY_FIELD)), cursor.getString(cursor.getColumnIndex(VALUE_FIELD)));

                        msg2.type = QUERYALL_AGREE;
                        ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                        oos.writeObject(msg2);
                        oos.flush();

                    }else if(msg.type.equals(DELETE)){
                        sqLiteDatabaseReader = messageSQLiteHelper.getReadableDatabase();
                        sqLiteDatabaseWriter.delete(TABLE_MESSAGE, "key = ?", new String[] {msg.key});

                    }else if(msg.type.equals(DELETEALL))
                        sqLiteDatabaseWriter.delete(TABLE_MESSAGE, null, null);

                } catch (Exception e) {
                    e.printStackTrace();
                }finally {
                    try {
                        socket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            String type = msgs[0];

            if (type.equals(JOIN)) {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(MANAGER_PORT));
                    Message msg = new Message();
                    msg.key = myPort;
                    msg.value = String.valueOf(Integer.parseInt(myPort)/2);
                    msg.type = JOIN;

                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                    oos.writeObject(msg);
                    oos.flush();

                    socket.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }else if(type.equals(JOIN_AGREE)){
                Message msg = new Message();
                msg.key = myPort;
                msg.dhtChord = dhtChord;
                msg.type = JOIN_AGREE;

                for(String[] curNode : dhtChord){
                    String curPort = curNode[0];
                    if(!curPort.equals(myPort)){
                        try {
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(curPort));
                            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                            oos.writeObject(msg);
                            oos.flush();

                            socket.close();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }else if(type.equals(INSERT)){

                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[1]));
                    Message msg = new Message();
                    msg.type = INSERT;
                    msg.key = msgs[2];
                    msg.value = msgs[3];

                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                    oos.writeObject(msg);
                    oos.flush();

                    socket.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }else if(type.equals(QUERY)) {

                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[1]));
                    Message msg = new Message();
                    msg.type = QUERY;
                    msg.key = msgs[2];
                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                    oos.writeObject(msg);
                    oos.flush();

                    ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                    Message msg2 = (Message) ois.readObject();
                    tempTable = msg2.table;
                    flag = 1;

                    socket.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }else if(type.equals(QUERYALL)) {

                try {
                    for(String[] curNode : dhtChord){
                        String curPort = curNode[0];
                        try {
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(curPort));
                            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                            Message msg = new Message();
                            msg.type = QUERYALL;
                            oos.writeObject(msg);
                            oos.flush();

                            ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                            Message msg2 = (Message) ois.readObject();
                            tableList.add(msg2.table);
                            flag++;

                            socket.close();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }

            }else if(type.equals(DELETE)) {

                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[1]));
                    Message msg = new Message();
                    msg.type = DELETE;
                    msg.key = msgs[2];
                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                    oos.writeObject(msg);
                    oos.flush();

                    socket.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }else if(type.equals(DELETEALL)){

                for(String[] curNode : dhtChord){
                    String curPort = curNode[0];
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(curPort));
                        ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                        Message msg = new Message();
                        msg.type = DELETEALL;
                        msg.key = curPort;
                        oos.writeObject(msg);
                        oos.flush();

                        socket.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            return null;
        }
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private void switchToClient(String type, String port){
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, type, port);
    }
}
