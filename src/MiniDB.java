
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MiniDB {
    private Map<String, Long> indexes; // 内存中的索引信息
    private DBFile dbFile; // 数据文件
    private String dirPath; // 数据目录
    private ReadWriteLock lock;

    public static final byte PUT = 1;
    public static final byte DEL = 2;
    public static final String FILE_NAME = "minidb.data";
    public static final String MERGE_FILE_NAME = "minidb.data.merge";

    public MiniDB(DBFile dbFile, String dirPath) {
        this.dbFile = dbFile;
        this.dirPath = dirPath;
        this.indexes = new HashMap<>();
        this.lock = new ReentrantReadWriteLock();
    }

    public static MiniDB Open(String dirPath) throws IOException {
        // 如果数据库目录不存在，则新建一个
        File dir = new File(dirPath);
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new IOException("Failed to create directory: " + dirPath);
            }
        }

        // 加载数据文件
        DBFile dbFile = DBFile.NewDBFile(dirPath);

        MiniDB db = new MiniDB(dbFile, dirPath);

        // 加载索引
        db.loadIndexesFromFile();
        return db;
    }

    // Merge 合并数据文件，在rosedb当中是 Reclaim 方法
    public void Merge() throws IOException {
        // 没有数据，忽略
        if (dbFile.offset == 0) {
            return;
        }

        long offset = 0;
        Entry e;
        byte[] key;
        Long off;
        byte mark;
        Map<String, Entry> validEntries = new HashMap<>();

        // 读取原数据文件中的 Entry
        while (true) {
            e = dbFile.Read(offset);
            if (e == null) {
                break;
            }
            key = e.getKey();
            off = indexes.get(new String(key));
            // 内存中的索引状态是最新的，直接对比过滤出有效的 Entry
            if (off != null && off == offset) {
                validEntries.put(new String(key), e);
            }
            offset += e.getSize();
        }

        if (!validEntries.isEmpty()) {
            // 新建临时文件
            DBFile mergeDBFile = DBFile.NewMergeDBFile(dirPath);

            lock.writeLock().lock();
            try {
                // 重新写入有效的 entry
                for (Entry entry : validEntries.values()) {
                    long writeOff = mergeDBFile.offset;
                    mergeDBFile.Write(entry);

                    // 更新索引
                    indexes.put(new String(entry.getKey()), writeOff);
                }

                // 获取文件名
                String dbFileName = "\\tmp\\minidb\\minidb.data";
//                String dbFile
//                Name = dbFile.file.getName();
                // 关闭文件
                dbFile.Close();
                // 删除旧的数据文件
                new File(dirPath, dbFileName).delete();

                // 获取文件名
//                String mergeDBFileName = mergeDBFile.file.getName();
                String mergeDBFileName = "\\tmp\\minidb\\minidb.data.merge";
                // 临时文件变更为新的数据文件
                new File(dirPath, mergeDBFileName).renameTo(new File(dirPath, FILE_NAME));

                dbFile = mergeDBFile;
            } finally {
                lock.writeLock().unlock();
            }
        }
    }

    // Put 写入数据
    public void Put(byte[] key, byte[] value) throws IOException {
        if (key == null || key.length == 0) {
            return;
        }

        lock.writeLock().lock();
        try {
            long offset = dbFile.offset;
            // 封装成 Entry
            Entry entry = new Entry(key, value, PUT);
            // 追加到数据文件当中
            dbFile.Write(entry);

            // 写到内存
            indexes.put(new String(key), offset);
        } finally {
            lock.writeLock().unlock();
        }
    }

    // Get 取出数据
    public byte[] Get(byte[] key) throws IOException {
        if (key == null || key.length == 0) {
            return null;
        }

        lock.readLock().lock();
        try {
            // 从内存当中取出索引信息
            Long offset = indexes.get(new String(key));
            // key 不存在
            if (offset == null) {
                throw new IOException(String.format("{key: %s不存在.}", new String(key)));
            }

            // 从磁盘中读取数据
            Entry e = dbFile.Read(offset);
            if (e != null && e.getMark() != DEL) {
                return e.getValue();
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    // Del 删除数据
    public void Del(byte[] key) throws IOException {
        if (key == null || key.length == 0) {
            return;
        }

        lock.writeLock().lock();
        try {
            // 从内存当中取出索引信息
            Long offset = indexes.get(new String(key));
            // key 不存在，忽略
            if (offset == null) {
                return;
            }

            // 封装成 Entry 并写入
            Entry e = new Entry(key, null, DEL);
            dbFile.Write(e);

            // 删除内存中的 key
            indexes.remove(new String(key));
        } finally {
            lock.writeLock().unlock();
        }
    }

    // 从文件当中加载索引
    private void loadIndexesFromFile() throws IOException {
        if (dbFile == null) {
            return;
        }

        long offset = 0;
        Entry e;
        while (true) {
            try {
                e = dbFile.Read(offset);
            } catch (EOFException err) {
                break;
            }
            if (e == null) {
                return;
            }
            byte[] key = e.getKey();

            // 设置索引状态
            indexes.put(String.valueOf(key), offset);

            if (e.getMark() == DEL) {
                // 删除内存中的 key
                indexes.remove(new String(key));
            }

            offset += e.getSize();
        }
    }

    // Close 关闭 db 实例
    public void Close() throws IOException {
        if (dbFile == null) {
            throw new IOException("invalid dbfile");
        }

        dbFile.Close();
    }
}