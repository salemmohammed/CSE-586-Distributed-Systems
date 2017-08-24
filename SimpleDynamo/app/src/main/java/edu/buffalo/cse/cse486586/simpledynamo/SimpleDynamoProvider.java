package edu.buffalo.cse.cse486586.simpledynamo;

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
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static edu.buffalo.cse.cse486586.simpledynamo.MessageSQLiteHelper.TABLE_MESSAGE;

public class SimpleDynamoProvider extends ContentProvider {
	private static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	Uri uri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider");
	private static final int SERVER_PORT = 10000;
	static final String REMOTE_PORT0 = "11108", REMOTE_PORT1 = "11112", REMOTE_PORT2 = "11116", REMOTE_PORT3 = "11120", REMOTE_PORT4 = "11124";
	static final String REMOTE_PORTS[] = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
	private static final String KEY_FIELD = "key", VALUE_FIELD = "value";
	private static final String INSERT = "2", QUERY = "3", QUERYALL = "4", QUERYALL_AGREE = "5", DELETE = "6", DELETEALL = "7", QUERY_OTHER = "8";
	public static final int TIMEOUT_VALUE = 5000;
	private static MessageSQLiteHelper messageSQLiteHelper;
	private static SQLiteDatabase sqLiteDatabaseWriter;
	private static SQLiteDatabase sqLiteDatabaseReader;

	String myPort;
	ArrayList<String[]> dhtChord;
	ArrayList<LinkedHashMap<String, String>> tableList;
	int flag = 0;

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		if (selection.equals("@"))
			return sqLiteDatabaseWriter.delete(TABLE_MESSAGE, null, null);
		else if(selection.equals("*"))
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, DELETEALL, selection);
		else{
			try {
				String targetPort = getTargetPort(genHash(selection));
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, DELETE, targetPort, selection);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, DELETE, getSucc(targetPort), selection);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, DELETE, getSucc(getSucc(targetPort)), selection);
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
		synchronized (this) {
			String key = values.getAsString(KEY_FIELD);
			String value = values.getAsString(VALUE_FIELD);
			try {
				String targetPort = getTargetPort(genHash(key));
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, INSERT, targetPort, key, value);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, INSERT, getSucc(targetPort), key, value);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, INSERT, getSucc(getSucc(targetPort)), key, value);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	public Uri localInsert(Uri uri, ContentValues values) {
		String key = values.getAsString(KEY_FIELD);
		String value = values.getAsString(VALUE_FIELD);
		Cursor cursor = sqLiteDatabaseWriter.rawQuery("select * from " + TABLE_MESSAGE + " where key=?", new String[]{key});

		if (!(cursor.moveToFirst()) || cursor.getCount() == 0) {
			Long newid = sqLiteDatabaseWriter.insert(MessageSQLiteHelper.TABLE_MESSAGE, null, values);
			Uri insertUri = ContentUris.withAppendedId(uri, newid);
			getContext().getContentResolver().notifyChange(insertUri, null);
			return insertUri;

		} else
			sqLiteDatabaseWriter.execSQL("UPDATE " + TABLE_MESSAGE + " SET value=? WHERE key =?", new String[]{value, key});
		return null;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		messageSQLiteHelper = new MessageSQLiteHelper(getContext());
		sqLiteDatabaseWriter = messageSQLiteHelper.getWritableDatabase();
		sqLiteDatabaseReader = messageSQLiteHelper.getReadableDatabase();
		dhtChord = new ArrayList<String[]>();
		tableList = new ArrayList<LinkedHashMap<String, String>>();

		if (getContext() != null) {
			TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
			String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
			myPort = String.valueOf((Integer.parseInt(portStr) * 2));

			for (int i = 0; i < REMOTE_PORTS.length; i++) {
				try {
					String[] temp = new String[]{REMOTE_PORTS[i], genHash(String.valueOf(Integer.parseInt(REMOTE_PORTS[i]) / 2))};
					dhtChord.add(temp);
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}
			}

			Collections.sort(dhtChord, new Comparator<String[]>() {
				@Override
				public int compare(String[] lhs, String[] rhs) {
					return lhs[1].compareTo(rhs[1]);
				}
			});

			try {
				ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
				new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
			} catch (IOException e) {
				Log.e(TAG, "Can't create a ServerSocket");
			}

			synchronized (this) {
				try {
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, QUERYALL, "*");

					while (flag < dhtChord.size()) {}

					LinkedHashMap<String, LinkedHashMap<String, Integer>> temp = new LinkedHashMap<String, LinkedHashMap<String, Integer>>();

					for (int i = 0; i < tableList.size(); i++) {
						for (Map.Entry<String, String> entry : tableList.get(i).entrySet()) {
							String curKey = entry.getKey();
							try {
								if (getTargetPort(genHash(curKey)).equals(myPort) || getTargetPort(genHash(curKey)).equals(getPred(myPort)) || getTargetPort(genHash(curKey)).equals(getPred(getPred(myPort)))) {
									if (!temp.containsKey(entry.getKey())) {
										temp.put(entry.getKey(), new LinkedHashMap<String, Integer>());
										temp.get(entry.getKey()).put(entry.getValue(), 1);
									} else if (!temp.get(entry.getKey()).containsKey(entry.getValue())) {
										temp.get(entry.getKey()).put(entry.getValue(), 1);
									} else
										temp.get(entry.getKey()).put(entry.getValue(), 1 + temp.get(entry.getKey()).get(entry.getValue()));
								}
							} catch (NoSuchAlgorithmException e) {
								e.printStackTrace();
							}
						}
					}

					for (Map.Entry<String, LinkedHashMap<String, Integer>> entry1 : temp.entrySet()) {
						int max = 0;
						String rightValue = null;
						for (Map.Entry<String, Integer> entry2 : entry1.getValue().entrySet()) {
							if (max < entry2.getValue()) {
								max = entry2.getValue();
								rightValue = entry2.getKey();
							}
						}

						ContentValues values = new ContentValues();
						values.put(KEY_FIELD, entry1.getKey());
						values.put(VALUE_FIELD, rightValue);
						localInsert(uri, values);
					}

					flag = 0;
					tableList.clear();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		return false;
	}

	private String getSucc(String curPort) {
		for(int i = 0; i < dhtChord.size(); i++){
			if(dhtChord.get(i)[0].equals(curPort)) {
				int succ1 = i + 1 > 4 ? i - 4 : i + 1;
				return dhtChord.get(succ1)[0];
			}
		}
		return myPort;
	}

	private String getPred(String curPort) {
		for(int i = 0; i < dhtChord.size(); i++){
			if(dhtChord.get(i)[0].equals(curPort)) {
				int succ1 = i - 1 >= 0 ? i - 1 : 4 - i;
				return dhtChord.get(succ1)[0];
			}
		}
		return myPort;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		MatrixCursor cursor = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});

		if (selection.equals("@")){

			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, QUERYALL, selection);

			while(flag < dhtChord.size()){}

			LinkedHashMap<String, LinkedHashMap<String, Integer>> temp = new LinkedHashMap<String, LinkedHashMap<String, Integer>>();

			for(int i = 0; i < tableList.size(); i ++){
				for(Map.Entry<String, String> entry : tableList.get(i).entrySet()){
					if(!temp.containsKey(entry.getKey())) {
						temp.put(entry.getKey(), new LinkedHashMap<String, Integer>());
						temp.get(entry.getKey()).put(entry.getValue(), 1);
					}else if(!temp.get(entry.getKey()).containsKey(entry.getValue())){
						temp.get(entry.getKey()).put(entry.getValue(), 1);
					}else
						temp.get(entry.getKey()).put(entry.getValue(), 1 + temp.get(entry.getKey()).get(entry.getValue()));
				}
			}

			for(Map.Entry<String, LinkedHashMap<String, Integer>> entry1 : temp.entrySet()){
				int max = 0, i = 0;
				String rightValue = null;
				for(Map.Entry<String, Integer> entry2 : entry1.getValue().entrySet()){
					if(max < entry2.getValue()){
						max = entry2.getValue();
						rightValue = entry2.getKey();
					}
					i++;
				}

				if(i < 2) continue;
				ContentValues values = new ContentValues();
				values.put(KEY_FIELD, entry1.getKey());
				values.put(VALUE_FIELD, rightValue);
				insert(uri, values);
			}

			flag = 0;
			tableList.clear();

			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			return sqLiteDatabaseReader.query(TABLE_MESSAGE, null, null, null, null, null, null);

		}else if(selection.equals("*")){

			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, QUERYALL, selection);

			while(flag < dhtChord.size()){}

			LinkedHashMap<String, String> temp = new LinkedHashMap<String, String>();

			for(int i = 0; i < tableList.size(); i ++){
				for(Map.Entry<String, String> entry : tableList.get(i).entrySet()){
					temp.put(entry.getKey(), entry.getValue());
				}
			}

			for(Map.Entry<String, String> entry : temp.entrySet())
				cursor.addRow(new String[]{entry.getKey(), entry.getValue()});

			flag = 0;

			if(cursor.getCount() == 0) return null;
			tableList.clear();
			return cursor;

		}else{

			try {
				synchronized (this) {

					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, QUERY, selection);

					while(flag < dhtChord.size()){}
					flag = 0;

					LinkedHashMap<String, Integer> temp = new LinkedHashMap<String, Integer>();
					int max = 0;

					for(int i = 0; i < tableList.size(); i ++){
						for(Map.Entry<String, String> entry : tableList.get(i).entrySet()){
							String tmpKey = entry.getValue();
							if (!temp.containsKey(tmpKey)) temp.put(tmpKey, 1);
							else temp.put(tmpKey, temp.get(tmpKey)+1);
							max = Math.max(max, temp.get(tmpKey));
						}
					}

					ContentValues values = new ContentValues();
					for(Map.Entry<String, Integer> entry : temp.entrySet()){
						if(entry.getValue() == max) {
							cursor.addRow(new String[]{selection, entry.getKey()});
							values.put(KEY_FIELD, selection);
							values.put(VALUE_FIELD, entry.getKey());
							break;
						}
					}

					insert(uri, values);
					tableList.clear();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		return cursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
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

					if(msg.type.equals(INSERT)){

						ContentValues values = new ContentValues();
						values.put(KEY_FIELD, msg.key);
						values.put(VALUE_FIELD, msg.value);
						localInsert(uri, values);

						ois.close();
					}else if(msg.type.equals(QUERY)){
						Cursor cur = sqLiteDatabaseReader.rawQuery("select * from " + TABLE_MESSAGE + " where key=?", new String[] {msg.key});
						Message msg2 = new Message();
						if(cur.getCount() != 0) {
							cur.moveToFirst();
							msg2.key = cur.getString(cur.getColumnIndex(KEY_FIELD));
							msg2.value = cur.getString(cur.getColumnIndex(VALUE_FIELD));
							msg2.table.put(msg2.key, msg2.value);
						}

						ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
						oos.writeObject(msg2);
						oos.flush();

						oos.close();
						ois.close();
					}/*else if(msg.type.equals(QUERY_OTHER)){
						Cursor cur = sqLiteDatabaseReader.query(TABLE_MESSAGE, null, null, null, null, null, null);
						Message msg2 = new Message();
						while(cur.moveToNext()) {
							msg2.table.put(cur.getString(cur.getColumnIndex(KEY_FIELD)), cur.getString(cur.getColumnIndex(VALUE_FIELD)));
						}

						ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
						oos.writeObject(msg2);
						oos.flush();

						oos.close();
						ois.close();
					}*/else if(msg.type.equals(QUERYALL)){
						Cursor cursor = sqLiteDatabaseReader.query(TABLE_MESSAGE, null, null, null, null, null, null);
						Message msg2 = new Message();

						while (cursor.moveToNext()) {
							String tmpKey = cursor.getString(cursor.getColumnIndex(KEY_FIELD));
							String tmpValue = cursor.getString(cursor.getColumnIndex(VALUE_FIELD));
							msg2.table.put(tmpKey, tmpValue);
						}

						msg2.type = QUERYALL_AGREE;
						ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
						oos.writeObject(msg2);
						oos.flush();

						oos.close();
						ois.close();
					}else if(msg.type.equals(DELETE)){
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

			if(type.equals(INSERT)){

				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[1]));
					socket.setSoTimeout(TIMEOUT_VALUE);
					Message msg = new Message();
					msg.type = INSERT;
					msg.key = msgs[2];
					msg.value = msgs[3];

					ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
					oos.writeObject(msg);
					oos.flush();

					oos.close();
					socket.close();
				} catch (Exception e) {
					e.printStackTrace();
				}

			}else if(type.equals(QUERY)) {

				try {
					for(String[] curNode : dhtChord){
						String curPort = curNode[0];
						try {
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(curPort));
							socket.setSoTimeout(TIMEOUT_VALUE);
							ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
							Message msg = new Message();
							msg.type = QUERY;
							msg.key = msgs[1];
							oos.writeObject(msg);
							oos.flush();

							ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
							Message msg2 = (Message) ois.readObject();
							tableList.add(msg2.table);
							flag++;

							oos.close();
							ois.close();
							socket.close();
						} catch (Exception e) {
							flag++;
							e.printStackTrace();
							continue;
						}
					}

				} catch (Exception e) {
					e.printStackTrace();
				}

			}/*else if(type.equals(QUERY_OTHER)) {
				Socket socket = null;
				Message msg = new Message();
				msg.type = QUERY_OTHER;
				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[1]));
					socket.setSoTimeout(TIMEOUT_VALUE);
					ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
					oos.writeObject(msg);
					oos.flush();

					ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
					Message msg2 = (Message) ois.readObject();

					for(Map.Entry<String, String> entry : msg2.table.entrySet()){
						String curKey = entry.getKey();
						if(getTargetPort(genHash(curKey)).equals(myPort) ||
								getTargetPort(genHash(curKey)).equals(getPred(myPort)) ||
								getTargetPort(genHash(curKey)).equals(getPred(getPred(myPort)))){
							String[] curValue = entry.getValue().split("#");
							ContentValues values = new ContentValues();
							values.put(KEY_FIELD, curKey);
							values.put(VALUE_FIELD, curValue[0]);
							localInsert(uri, values);
						}
					}

					ois.close();
					oos.close();
					socket.close();
				} catch (Exception e) {
					e.printStackTrace();
				}

			}*/else if(type.equals(QUERYALL)) {

				try {
					for(String[] curNode : dhtChord){
						String curPort = curNode[0];
						try {
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(curPort));
							socket.setSoTimeout(TIMEOUT_VALUE);
							ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
							Message msg = new Message();
							msg.type = QUERYALL;
							oos.writeObject(msg);

							oos.flush();

							ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
							Message msg2 = (Message) ois.readObject();
							tableList.add(msg2.table);
							flag++;

							oos.close();
							ois.close();
							socket.close();
						} catch (Exception e) {
							flag++;
							e.printStackTrace();
							continue;
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

					oos.close();
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
						socket.setSoTimeout(TIMEOUT_VALUE);
						ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
						Message msg = new Message();
						msg.type = DELETEALL;
						msg.key = curPort;
						oos.writeObject(msg);
						oos.flush();

						oos.close();
						socket.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
			return null;
		}
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
}
