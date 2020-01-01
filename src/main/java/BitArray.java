/*
 * Copyright 2018 University of California, Riverside
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.sun.xml.internal.messaging.saaj.packaging.mime.util.BASE64DecoderStream;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Base64InputStream;
import sun.misc.BASE64Decoder;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import javax.xml.bind.DatatypeConverter;

/**
 * An array of bits which is stored efficiently in memory and can be serialized
 * or deserialized using Hadoop serialization framework.
 * @author Ahmed Eldawy
 *
 */
public class BitArray {

  /**Number of bits per entry*/
  private static final int BitsPerEntry = 64;
  
  /**Condensed representation of all data*/
  protected long[] entries;
  
  /**Total number of bits stores in this array*/
  protected long size;

  /**Default constructor is needed for deserialization*/
  public BitArray() {
  }
  
  /**
   * Initializes a bit array with the given capacity in bits.
   * @param size Total number of bits in the array. All initialized to false.
   */
  public BitArray(long size) {
    this.size = size;
    entries = new long[(int) ((size + BitsPerEntry - 1) / BitsPerEntry)];
  }

  @Override
  public BitArray clone() {
    BitArray replica = new BitArray();
    replica.size = this.size;
    replica.entries = this.entries.clone();
    return replica;
  }

  /**
   * Sets the bit at position <code>i</code>
   * @param i
   * @param b
   */
  public void set(long i, boolean b) {
    int entry = (int) (i / BitsPerEntry);
    int offset = (int) (i % BitsPerEntry);
    if (b) {
      entries[entry] |= (1L << offset);
    } else {
      entries[entry] &= ~(1L << offset);
    }
  }

  /**
   * Resize the array to have at least the given new size without losing the
   * current data
   * @param newSize
   */
  public void resize(long newSize) {
    if (newSize > size) {
      // Resize needed
      int newArraySize = (int) ((newSize + BitsPerEntry - 1) / BitsPerEntry);
      if (newArraySize > entries.length) {
        long[] newEntries = new long[newArraySize];
        System.arraycopy(entries, 0, newEntries, 0, entries.length);
        entries = newEntries;
      }
      size = newSize;
    }
  }
  
  /**
   * Returns the boolean at position <code>i</code>
   * @param i
   * @return
   */
  public boolean get(long i) {
    int entry = (int) (i / BitsPerEntry);
    int offset = (int) (i % BitsPerEntry);
    System.out.println("entry:"+ entry+" offset: "+ offset);
    return (entries[entry] & (1L << offset)) != 0;
  }

  /**
   * Count number of set bits in the bit array.
   * Code adapted from
   * https://codingforspeed.com/a-faster-approach-to-count-set-bits-in-a-32-bit-integer/
   * @return
   */
  public long countOnes() {
    long totalCount = 0;
    for (long i : entries) {
      i = i - ((i >>> 1) & 0x5555555555555555L);
      i = (i & 0x3333333333333333L) + ((i >>> 2) & 0x3333333333333333L);
      i = (i + (i >>> 4)) & 0x0f0f0f0f0f0f0f0fL;
      i = i + (i >>> 8);
      i = i + (i >>> 16);
      i = i + (i >>> 32);
      totalCount += i & 0x7f;
    }
    return totalCount;
  }
/*
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(size);
    int numEntriesToWrite = (int) ((size + BitsPerEntry - 1) / BitsPerEntry);
    ByteBuffer bbuffer = ByteBuffer.allocate(numEntriesToWrite * 8);
    for (int i = 0; i < numEntriesToWrite; i++)
      bbuffer.putLong(entries[i]);
    // We should fill up the bbuffer
    assert bbuffer.remaining() == 0;
    out.write(bbuffer.array(), bbuffer.arrayOffset(), bbuffer.position() - bbuffer.arrayOffset());
  }

  @Override*/

  public void read(String path) throws IOException {
    int numEntriesToRead = (int) ((size + BitsPerEntry - 1) / BitsPerEntry);
    InputStream finput = new FileInputStream(path);
    //Base64InputStream base64InputStream = new Base64InputStream(finput, false);

    byte[] imageBytes = new byte[numEntriesToRead*8];
    finput.read(imageBytes, 0, imageBytes.length);

    //String imageStr = new String(imageBytes);
    ByteBuffer bbuffer = ByteBuffer.wrap(imageBytes);
    for (int i = 0; i < numEntriesToRead; i++) {
      entries[i] = bbuffer.getLong();
    }
    finput.close();
    /*
    int numEntriesToRead = (int) ((size + BitsPerEntry - 1) / BitsPerEntry);
    FileInputStream fIS = new FileInputStream(new File(path));

    int _byte;
    byte[] buffer = new byte[numEntriesToRead * 8];

    ByteBuffer bb = ByteBuffer.wrap(buffer);
    while((_byte = fIS.read()) != -1) {
      bb.putInt(_byte);
    }
    buffer = BASE64DecoderStream.decode(buffer);
    for (int i = 0; i < numEntriesToRead; i++)
      entries[i] = bb.getLong();*/

  }
  public void readFields(DataInput in) throws IOException {
    size = in.readLong();
    int numEntriesToRead = (int) ((size + BitsPerEntry - 1) / BitsPerEntry);
    if (entries == null || entries.length < numEntriesToRead)
      entries = new long[numEntriesToRead];
    byte[] buffer = new byte[numEntriesToRead * 8];
    in.readFully(buffer);
    ByteBuffer bbuffer = ByteBuffer.wrap(buffer);

    for (int i = 0; i < numEntriesToRead; i++)
      entries[i] = bbuffer.getLong();
    assert !bbuffer.hasRemaining();
  }

  public long size() {
    return size;
  }

  public void fill(boolean b) {
    long fillValue = b ? 0xffffffffffffffffL : 0;
    for (int i = 0; i < entries.length; i++)
      entries[i] = fillValue;
  }

  public BitArray invert() {
    BitArray result = new BitArray();
    result.entries = new long[this.entries.length];
    result.size = this.size();
    for (int i = 0; i < entries.length; i++)
      result.entries[i] = ~this.entries[i];
    return result;
  }

  /**
   * Computes the bitwise OR of two bitmasks of the same size
   * @param other
   * @return
   */
  public BitArray or(BitArray other) {
    if (this.size != other.size)
      throw new RuntimeException("Cannot OR two BitArrays of different sizes");
    BitArray result = new BitArray();
    result.entries = new long[this.entries.length];
    result.size = this.size;
    for (int i = 0; i < entries.length; i++)
      result.entries[i] = this.entries[i] | other.entries[i];
    return result;
  }

  public void inplaceOr(BitArray other) {
    if (this.size != other.size)
      throw new RuntimeException("Cannot OR two BitArrays of different sizes");
    for (int i = 0; i < entries.length; i++)
      this.entries[i] |= other.entries[i];
  }

  /**
   * Sets the given range to the same value
   * @param start the first offset of set (inclusive)
   * @param end the end of the range (exclusive)
   * @param value the value to set in the given range
   */
  public void setRange(int start, int end, boolean value) {
    // TODO make it more efficient for long ranges by setting all values in the same entry together
    while (start < end)
      set(start++, value);
  }

  /**
   * Computes a logical OR between a specific range in this bit array and another bit array.
   * @param destinationOffset
   * @param other
   * @param sourceOffset
   * @param width
   */
  public void inplaceOr(long destinationOffset, BitArray other, long sourceOffset, int width) {
    // TODO make it more efficient for long ranges by ORing long entries rather than bit-by-bit
    while (width-- > 0) {
      this.set(destinationOffset + width, other.get(sourceOffset + width));
    }
  }
}
