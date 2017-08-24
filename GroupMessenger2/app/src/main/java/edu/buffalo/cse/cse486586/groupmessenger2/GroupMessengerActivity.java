package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.PriorityQueue;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108", REMOTE_PORT1 = "11112", REMOTE_PORT2 = "11116", REMOTE_PORT3 = "11120", REMOTE_PORT4 = "11124";
    static final String REMOTE_PORTS[] = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
    static final int SERVER_PORT = 10000;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    Uri uri = Uri.parse("content://edu.buffalo.cse.cse486586.groupmessenger2.provider");
    int key = 0, counter = 0;
    ArrayList<Socket> sockets = new ArrayList<Socket>();
    static String myPortCopy="";
    PriorityQueue<Message> pq=new PriorityQueue<Message>(5);

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        final TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        myPortCopy = myPort;

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }

        final Button button = (Button) findViewById(R.id.button4);
        final EditText editText = (EditText) findViewById(R.id.editText1);

        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        button.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {

                String msg = editText.getText().toString();
                if(!msg.equals("")){
                    editText.setText(""); // This is one way to reset the input box.
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
                }
            }
        });
    }
    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            String port = "";
            Socket socket = null;
            while(true) {
                try {
                    socket = serverSocket.accept();
                    ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                    Message receivedMessage = (Message) ois.readObject();

                    port = receivedMessage.getPort();
                    int proposalOrder = receivedMessage.getOrder();
                    counter++;
                    Message sentMessage = proposalOrder > counter ? new Message(receivedMessage.getContent(), proposalOrder, receivedMessage.getPort()) : new Message(receivedMessage.getContent(), counter, myPortCopy);
                    counter = Math.max(counter, proposalOrder);

                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                    oos.writeObject(sentMessage);
                    oos.flush();
                    pq.add(sentMessage);

                    ObjectInputStream ois2 = new ObjectInputStream(socket.getInputStream());
                    Message aggreedMessage = (Message) ois2.readObject();
                    aggreedMessage.setAgreed(true);

                    for (Message curr : pq) {
                        if (curr.getContent().equals(aggreedMessage.getContent())) {
                            pq.remove(curr);
                            Message msg = new Message(aggreedMessage.getContent(), aggreedMessage.getOrder(), aggreedMessage.getPort());
                            msg.setAgreed(true);
                            pq.offer(msg);
                        }
                    }

                    if (pq.peek().isAgreed() && pq.peek().getOrder() != -1) {
                        Message deliverMessage = pq.poll();
                        publishProgress(deliverMessage.getContent());

                    }
                } catch(Exception e){
                    for (Message curr : pq){
                        if (curr.getPort().equals(port)) pq.remove(curr);
                    }
                    e.printStackTrace();
                    continue;
                }finally {
                    try {
                        socket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String strReceived = strings[0].trim();

            ContentValues cv = new ContentValues();
            cv.put(KEY_FIELD, (key++)+ "");
            cv.put(VALUE_FIELD, strReceived);
            getContentResolver().insert(uri, cv);

            TextView textView = (TextView) findViewById(R.id.textView1);
            textView.append(strReceived + "\t\n");

            return;
        }
    }

    /***
     * ClientTask is an AsyncTask that should send a string over the network.
     * It is created by ClientTask.executeOnExecutor() call whenever OnKeyListener.onKey() detects
     * an enter key press event.
     *
     * @author stevko
     *
     */
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            String msgToSend = msgs[0];
            counter++;
            Message aggredMessage = new Message("default message", -1, "10000");

            for(int i = 0; i < REMOTE_PORTS.length; i++) {
                try {

                    String remotePort = REMOTE_PORTS[i];

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));

                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                    Message proposedMessage = new Message(msgToSend, counter, remotePort);
                    oos.writeObject(proposedMessage);
                    oos.flush();

                    ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                    Message feedbackMessage = (Message) ois.readObject();
                    if(aggredMessage.compareTo(feedbackMessage) <= 0) aggredMessage = feedbackMessage;
                    sockets.add(socket);

                }catch (Exception e){
                    e.printStackTrace();
                }
            }

            for(Socket socket : sockets){
                try {
                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                    oos.writeObject(aggredMessage);
                    oos.flush();
                } catch (Exception e){
                    e.printStackTrace();
                }finally {
                    try {
                        socket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            sockets.clear();
            return null;
        }
    }
}