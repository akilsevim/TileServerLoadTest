import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;


public class BloomFilter extends BitArray {

    /**Logger for this class*/
    private static final Log LOG = LogFactory.getLog(BloomFilter.class);

    //private BitArray bits = new BitArray();
    private int m;
    private int k;
    private double p = 0.01;
    private double n;
    private long countApprx = 0;

    /**Default constructor is needed for deserialization*/
    public BloomFilter() { }

    /**
     * Create a bloom filter with given parameters.
     *
     * @param m number of bits
     * @param p false positive rate
     * @param k number of hash functions
     */
    public BloomFilter(int m, double p, int k) {
        super(m);
        this.m = m;
        this.p = p;
        this.k = k;

        this.n = Math.ceil(m / (-k / Math.log(1 - Math.exp(Math.log(p) / k))));
    }

    public void put(long element) {
        int hash1 = MurmurHash3_32_Long(element, 0);
        int hash2 = MurmurHash3_32_Long(element, 1);

        long index;
        boolean newValue = false;
        for (int i = 0; i < k; i++) {
            index = Math.abs(((long)hash1 + (long)i * (long)hash2) % m);
            if(!get(index)) newValue = true;
            set(index, true);
        }
        if(newValue) countApprx++;
    }

    public boolean mightContain(long element) {
        /*int hash1 = MurmurHash3_32_Long(element, 0);
        int hash2 = MurmurHash3_32_Long(element, 1);

        long index;
        boolean newValue = false;
        for (int i = 0; i < k; i++) {
            index = Math.abs(((long)hash1 + (long)i * (long)hash2) % m);
            if(!get(index)) newValue = true;
            set(index, true);
        }
        if(newValue) countApprx++;*/
        return true;
    }

    public void merge(BloomFilter otherFilter) {
        inplaceOr(otherFilter);
        countApprx += otherFilter.countApprx;
    }

    public long getCountApprx() {
        return countApprx;
    }

    public String getBitsAsBase64() {
        int numEntriesToWrite = (int) ((size + 64 - 1) / 64);
        ByteBuffer bbuffer = ByteBuffer.allocate(numEntriesToWrite * 8);
        for (int i = 0; i < numEntriesToWrite; i++)
            bbuffer.putLong(entries[i]);
        // We should fill up the bbuffer
        assert bbuffer.remaining() == 0;
        return Base64.getEncoder().encodeToString(bbuffer.array());
    }

    private int MurmurHash3_32_Long(long key, int seed) {
        int low = (int) key;
        int high = (int) (key >>> 32);

        //Constants to calculate MurmurHash3_32_Long
        int c1 = 0xcc9e2d51;
        int c2 = 0x1b873593;

        int k1 = low * c1;
        k1 = Integer.rotateLeft(k1, 15);
        k1 *= c2;

        int h1 = seed ^ k1;
        h1 = Integer.rotateLeft(h1, 13);
        h1 = (h1 * 5) + 0xe6546b64;

        k1 = high * c1;
        k1 = Integer.rotateLeft(k1, 15);
        k1 *= c2;

        h1 ^= k1;
        h1 = Integer.rotateLeft(h1, 13);
        h1 = (h1 * 5) + 0xe6546b64;

        //For long type (8 byte)
        h1 ^= 8;

        h1 ^= h1 >>> 16;
        h1 *= 0x85ebca6b;
        h1 ^= h1 >>> 13;
        h1 *= 0xc2b2ae35;
        h1 ^= h1 >>> 16;

        return h1;
    }
}
