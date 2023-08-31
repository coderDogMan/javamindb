import java.io.IOException;
import java.util.Optional;

public class Main {
    public static void main(String[] args) throws IOException {
        MiniDB db = MiniDB.Open("/tmp/minidb");

        byte[] key = "dbname".getBytes();
        byte[] value = "minidb".getBytes();


        db.Put(key, value);

        byte[] get = db.Get(key);
        System.out.println(new String(get));

//        db.Del(key);

        db.Merge();


    }
}
