
import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;

public class DBFile {
    private static final String FileName = "minidb.data";
    private static final String MergeFileName = "minidb.data.merge";

    private RandomAccessFile file;
    public long offset;

    private DBFile(RandomAccessFile file, long offset) {
        this.file = file;
        this.offset = offset;
    }

    private static DBFile newInternal(String fileName) throws IOException {
        RandomAccessFile file = new RandomAccessFile(fileName, "rw");
        long size = Files.size(Paths.get(fileName));
        return new DBFile(file, size);
    }

    public static DBFile NewDBFile(String path) throws IOException {
        String fileName = Paths.get(path, FileName).toString();
        return newInternal(fileName);
    }

    public static DBFile NewMergeDBFile(String path) throws IOException {
        String fileName = Paths.get(path, MergeFileName).toString();
        return newInternal(fileName);
    }

    public Entry Read(long offset) throws IOException {
        byte[] buf = new byte[Entry.entryHeaderSize];
        file.seek(offset);
        try {
            file.readFully(buf);
        } catch (EOFException err) {
            return null;
        }
        Entry e = Entry.decode(buf);

        offset += Entry.entryHeaderSize;
        if (e.getKeySize() > 0) {
            byte[] key = new byte[e.getKeySize()];
            file.seek(offset);
            try {
                file.readFully(key);
            } catch (EOFException err) {
                return null;
            }
            e.setKey(key);
        }

        offset += e.getKeySize();
        if (e.getValueSize() > 0) {
            byte[] value = new byte[e.getValueSize()];
            file.seek(offset);
            try {
                file.readFully(value);
            } catch (EOFException err) {
                return null;
            }
            e.setValue(value);
        }

        return e;
    }

    public void Write(Entry e) throws IOException {
        byte[] enc = e.encode();
        file.seek(offset);
        file.write(enc);
        offset += e.getSize();
    }

    public void Close() throws IOException {
        file.close();
    }
}