package edu.buffalo.cse.cse486586.groupmessenger1;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.UriMatcher;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.util.Log;

import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.Map;
import java.util.Observable;
import java.util.Set;

import static edu.buffalo.cse.cse486586.groupmessenger1.MessageSQLiteHelper.TABLE_MESSAGE;

/**
 * GroupMessengerProvider is a key-value table. Once again, please note that we do not implement
 * full support for SQL as a usual ContentProvider does. We re-purpose ContentProvider's interface
 * to use it as a key-value table.
 * 
 * Please read:
 * 
 * http://developer.android.com/guide/topics/providers/content-providers.html
 * http://developer.android.com/reference/android/content/ContentProvider.html
 * 
 * before you start to get yourself familiarized with ContentProvider.
 * 
 * There are two methods you need to implement---insert() and query(). Others are optional and
 * will not be tested.
 * 
 * @author stevko
 *
 */
public class GroupMessengerProvider extends ContentProvider {

    /*------------------------my test code--------------------------------*/
    private MessageSQLiteHelper messageSQLiteHelper;
    private SQLiteDatabase sqLiteDatabaseWriter;
    private SQLiteDatabase sqLiteDatabaseReader;
    static int count = 0;

    public MessageSQLiteHelper getMessageSQLiteHelper() {
        return messageSQLiteHelper;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // You do not need to implement this.
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // You do not need to implement this.
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        /*
         * TODO: You need to implement this method. Note that values will have two columns (a key
         * column and a value column) and one row that contains the actual (key, value) pair to be
         * inserted.
         * 
         * For actual storage, you can use any option. If you know how to use SQL, then you can use
         * SQLite. But this is not a requirement. You can use other storage options, such as the
         * internal storage option that we used in PA1. If you want to use that option, please
         * take a look at the code for PA1.
         */

        Log.i("count", (count++) + "");
        Object set[] = values.valueSet().toArray();
        String curKey = (String)((Map.Entry) set[1]).getValue();
        String curValue = (String)((Map.Entry) set[0]).getValue();
        Log.i("uri", uri.toString());
        sqLiteDatabaseWriter = messageSQLiteHelper.getWritableDatabase();
        Cursor cursor = sqLiteDatabaseWriter.rawQuery("select * from " + TABLE_MESSAGE + " where key=?", new String[] { curKey });

        //if the key is not in the existing table, we insert this new entry to the table
        if (!(cursor.moveToFirst()) || cursor.getCount() == 0) {
            Long newid = sqLiteDatabaseWriter.insert(MessageSQLiteHelper.TABLE_MESSAGE, null, values);
            Uri insertUri = ContentUris.withAppendedId(uri, newid);
            getContext().getContentResolver().notifyChange(insertUri, null);
            return insertUri;
        }else{  //if the key is already in existing table, we update its corresponding value
            sqLiteDatabaseWriter.execSQL("UPDATE " + TABLE_MESSAGE + " SET value=? WHERE key =?", new String[] {curValue, curKey});
        }

        return uri;
    }


    @Override
    public boolean onCreate() {
        // If you need to perform any one-time initialization task, please do it here.
        messageSQLiteHelper = new MessageSQLiteHelper(getContext());
        return false;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // You do not need to implement this.
        return 0;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        /*
         * TODO: You need to implement this method. Note that you need to return a Cursor object
         * with the right format. If the formatting is not correct, then it is not going to work.
         *
         * If you use SQLite, whatever is returned from SQLite is a Cursor object. However, you
         * still need to be careful because the formatting might still be incorrect.
         *
         * If you use a file storage option, then it is your job to build a Cursor * object. I
         * recommend building a MatrixCursor described at:
         * http://developer.android.com/reference/android/database/MatrixCursor.html
         */
        Log.v("query", selection);
        sqLiteDatabaseReader = messageSQLiteHelper.getReadableDatabase();
        Cursor cursor = sqLiteDatabaseReader.rawQuery("select * from " + TABLE_MESSAGE + " where key=?", new String[] {selection});
        cursor.moveToFirst();
        return cursor;
    }
}
