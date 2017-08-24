package edu.buffalo.cse.cse486586.groupmessenger1;

import android.content.Context;
import android.database.DatabaseErrorHandler;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

/**
 * Created by xiaoxin on 2/14/17.
 */

public class MessageSQLiteHelper extends SQLiteOpenHelper {

    public static final String TABLE_MESSAGE = "message";
    public static final String MESSAGE_KEY = "key";
    public static final String MESSAGE_VALUE = "value";
    private static final String DATABASE_NAME = "groupmessage.db";
    private static final int DATABASE_VERSION = 1;

    private static final String CREATETABLE = "CREATE TABLE IF NOT EXISTS " + TABLE_MESSAGE + " ( " + MESSAGE_KEY  + " TEXT NOT NULL, " + MESSAGE_VALUE + " TEXT NOT NULL ) ";

    public MessageSQLiteHelper(Context context) {
        super(context, DATABASE_NAME, null, DATABASE_VERSION);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL(CREATETABLE);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {

    }


}
