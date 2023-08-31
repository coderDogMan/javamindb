
import java.nio.ByteBuffer;

public class Entry {
    public static final int entryHeaderSize = 10;

    public static final short PUT = 0;
    public static final short DEL = 1;

    private byte[] key;
    private byte[] value;
    private int keySize;
    private int valueSize;
    private short mark;

    public Entry(byte[] key, byte[] value, short mark) {
        this.key = key;
        this.value = value;
        if (key != null)
            this.keySize = key.length;
        if (value != null)
            this.valueSize = value.length;
        this.mark = mark;
    }

    public long getSize() {
        return entryHeaderSize + keySize + valueSize;
    }

    public byte[] encode() {
        ByteBuffer buf = ByteBuffer.allocate((int) getSize());
        buf.putInt(keySize);
        buf.putInt(valueSize);
        buf.putShort(mark);
        buf.put(key);
        if (value != null)
            buf.put(value);
        return buf.array();
    }

    public static Entry decode(byte[] buf) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(buf);
        int ks = byteBuffer.getInt();
        int vs = byteBuffer.getInt();
        short mark = byteBuffer.getShort();
        Entry entry = new Entry(null, null, mark);
        entry.setKeySize(ks);
        entry.setValueSize(vs);
        return entry;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public int getKeySize() {
        return keySize;
    }

    public void setKeySize(int keySize) {
        this.keySize = keySize;
    }

    public int getValueSize() {
        return valueSize;
    }

    public void setValueSize(int valueSize) {
        this.valueSize = valueSize;
    }

    public short getMark() {
        return mark;
    }

    public void setMark(short mark) {
        this.mark = mark;
    }
}