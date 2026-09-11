/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.memory.ffm;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.arrow.memory.util.MemoryUtilAccessor;

/**
 * {@link MemoryUtilAccessor} backed by {@code java.lang.foreign} ({@link MemorySegment}/{@link
 * Arena}). Does not use {@code sun.misc.Unsafe} or reflection into {@code java.nio} internals, so
 * it requires neither {@code --add-opens} nor {@code sun.misc.Unsafe} availability.
 *
 * <p>{@link #allocateMemory}/{@link #freeMemory} are provided for standalone callers of {@link
 * org.apache.arrow.memory.util.MemoryUtil#allocateMemory}/{@code #freeMemory}; each call gets its
 * own {@link Arena}, tracked by address so {@link #freeMemory} can close the right one.
 * Allocation-manager-owned memory instead goes through {@link FfmAllocationManager}, which holds
 * its {@link Arena} directly rather than round-tripping through this map.
 */
public final class FfmMemoryAccessor implements MemoryUtilAccessor {

  public static final MemoryUtilAccessor INSTANCE = new FfmMemoryAccessor();

  private static final ConcurrentHashMap<Long, Arena> STANDALONE_ARENAS = new ConcurrentHashMap<>();

  private FfmMemoryAccessor() {}

  private static MemorySegment segment(long address, long byteSize) {
    return MemorySegment.ofAddress(address).reinterpret(byteSize);
  }

  private static int checkedInt(long value) {
    if (value < 0 || value > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("value out of int range: " + value);
    }
    return (int) value;
  }

  @Override
  public long allocateMemory(long bytes) {
    Arena arena = Arena.ofShared();
    long address = arena.allocate(bytes).address();
    STANDALONE_ARENAS.put(address, arena);
    return address;
  }

  @Override
  public void freeMemory(long address) {
    Arena arena = STANDALONE_ARENAS.remove(address);
    if (arena != null) {
      arena.close();
    }
  }

  @Override
  public byte getByte(long address) {
    return segment(address, Byte.BYTES).get(ValueLayout.JAVA_BYTE, 0);
  }

  @Override
  public void putByte(long address, byte value) {
    segment(address, Byte.BYTES).set(ValueLayout.JAVA_BYTE, 0, value);
  }

  @Override
  public short getShort(long address) {
    return segment(address, Short.BYTES).get(ValueLayout.JAVA_SHORT_UNALIGNED, 0);
  }

  @Override
  public void putShort(long address, short value) {
    segment(address, Short.BYTES).set(ValueLayout.JAVA_SHORT_UNALIGNED, 0, value);
  }

  @Override
  public int getInt(long address) {
    return segment(address, Integer.BYTES).get(ValueLayout.JAVA_INT_UNALIGNED, 0);
  }

  @Override
  public void putInt(long address, int value) {
    segment(address, Integer.BYTES).set(ValueLayout.JAVA_INT_UNALIGNED, 0, value);
  }

  @Override
  public long getLong(long address) {
    return segment(address, Long.BYTES).get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
  }

  @Override
  public void putLong(long address, long value) {
    segment(address, Long.BYTES).set(ValueLayout.JAVA_LONG_UNALIGNED, 0, value);
  }

  @Override
  public void setMemory(long address, long bytes, byte value) {
    segment(address, bytes).fill(value);
  }

  @Override
  public void copyMemory(long srcAddress, long destAddress, long bytes) {
    MemorySegment.copy(segment(srcAddress, bytes), 0, segment(destAddress, bytes), 0, bytes);
  }

  @Override
  public void copyToMemory(byte[] src, long srcIndex, long destAddress, long bytes) {
    MemorySegment.copy(
        src,
        checkedInt(srcIndex),
        segment(destAddress, bytes),
        ValueLayout.JAVA_BYTE,
        0,
        checkedInt(bytes));
  }

  @Override
  public void copyFromMemory(long srcAddress, byte[] dest, long destIndex, long bytes) {
    MemorySegment.copy(
        segment(srcAddress, bytes),
        ValueLayout.JAVA_BYTE,
        0,
        dest,
        checkedInt(destIndex),
        checkedInt(bytes));
  }

  @Override
  public int getInt(byte[] bytes, int index) {
    return MemorySegment.ofArray(bytes).get(ValueLayout.JAVA_INT_UNALIGNED, index);
  }

  @Override
  public long getLong(byte[] bytes, int index) {
    return MemorySegment.ofArray(bytes).get(ValueLayout.JAVA_LONG_UNALIGNED, index);
  }

  @Override
  public long getByteBufferAddress(ByteBuffer buf) {
    return MemorySegment.ofBuffer(buf).address();
  }

  @Override
  public ByteBuffer directBuffer(long address, int capacity) {
    if (capacity < 0) {
      throw new IllegalArgumentException("Capacity is negative, has to be positive or 0");
    }
    return segment(address, capacity).asByteBuffer();
  }
}
