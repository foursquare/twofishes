/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.foursquare.twofishes.io;

import java.util.ArrayList;
import java.util.Arrays;
import java.io.EOFException;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/** A file-based map from keys to values.
 *
 * <p>A map is a directory containing two files, the <code>data</code> file,
 * containing all keys and values in the map, and a smaller <code>index</code>
 * file, containing a fraction of the keys.  The fraction is determined by
 * {@link Writer#getIndexInterval()}.
 *
 * <p>The index file is read entirely into memory.  Thus key implementations
 * should try to keep themselves small.
 *
 * <p>Map files are created by adding entries in-order.  To maintain a large
 * database, perform updates by copying the previous version of a database and
 * merging in a sorted change list, to create a new version of the database in
 * a new file.  Sorting large change lists can be done with {@link
 * SequenceFile.Sorter}.
 */
public class MapFileConcurrentReader {
  private static final Log LOG = LogFactory.getLog(MapFileConcurrentReader.class);

  /** Number of index entries to skip between each entry.  Zero by default.
   * Setting this to values larger than zero can facilitate opening large map
   * files using less memory. */
  private int INDEX_SKIP = 0;

  private WritableComparator comparator;

  // the data, on disk
  private ThreadLocal<SequenceFile.Reader> data;
  private ArrayList<SequenceFile.Reader> allDataFiles = new ArrayList<SequenceFile.Reader>();
  private SequenceFile.Reader index;
  long firstPosition = -1;

  // whether the index Reader was closed
  private boolean indexClosed = false;

  // the index, in memory
  private int count = -1;
  private WritableComparable[] keys;
  private long[] positions;

  public MapFileConcurrentReader(Path dir, Configuration conf,
                SequenceFile.Reader.Option... opts) throws IOException {

    INDEX_SKIP = conf.getInt("io.map.index.skip", 0);
    open(dir, comparator, conf, opts);
  }

  protected synchronized void open(Path dir,
                                   WritableComparator comparator,
                                   final Configuration conf,
                                   final SequenceFile.Reader.Option... options
                                   ) throws IOException {
    final Path dataFile = new Path(dir, MapFile.DATA_FILE_NAME);
    final Path indexFile = new Path(dir, MapFile.INDEX_FILE_NAME);

    // open the data
    this.data = new ThreadLocal<SequenceFile.Reader>() {
      protected SequenceFile.Reader initialValue() {
        try {
          SequenceFile.Reader r = createDataFileReader(dataFile, conf, options);
          LOG.info("opened new SequenceFile.Reader for " + dataFile);
          synchronized(this) {
            allDataFiles.add(r);
          }
          return r;
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
      }
    };
    this.firstPosition = data.get().getPosition();

    this.comparator =
      WritableComparator.get(data.get().getKeyClass().
                               asSubclass(WritableComparable.class));

    // open the index
    SequenceFile.Reader.Option[] indexOptions =
      Options.prependOptions(options, SequenceFile.Reader.file(indexFile));
    this.index = new SequenceFile.Reader(conf, indexOptions);
  }

  /**
   * Override this method to specialize the type of
   * {@link SequenceFile.Reader} returned.
   */
  protected SequenceFile.Reader
    createDataFileReader(Path dataFile, Configuration conf,
                         SequenceFile.Reader.Option... options
                         ) throws IOException {
    SequenceFile.Reader.Option[] newOptions =
      Options.prependOptions(options, SequenceFile.Reader.file(dataFile));
    return new SequenceFile.Reader(conf, newOptions);
  }

  private void readIndex() throws IOException {
    // read the index entirely into memory
    if (this.keys != null)
      return;
    this.count = 0;

    this.positions = new long[1024];

    try {
      int skip = INDEX_SKIP;
      LongWritable position = new LongWritable();
      WritableComparable lastKey = null;
      long lastIndex = -1;
      ArrayList<WritableComparable> keyBuilder = new ArrayList<WritableComparable>(1024);
      while (true) {
        WritableComparable k = comparator.newKey();

        if (!index.next(k, position))
          break;

        // check order to make sure comparator is compatible
        if (lastKey != null && comparator.compare(lastKey, k) > 0)
          throw new IOException("key out of order: "+k+" after "+lastKey);
        lastKey = k;
        if (skip > 0) {
          skip--;
          continue;                             // skip this entry
        } else {
          skip = INDEX_SKIP;                    // reset skip
        }

	  // don't read an index that is the same as the previous one. Block
	  // compressed map files used to do this (multiple entries would point
	  // at the same block)
	  if (position.get() == lastIndex)
	    continue;

        if (count == positions.length) {
	    positions = Arrays.copyOf(positions, positions.length * 2);
        }

        keyBuilder.add(k);
        positions[count] = position.get();
        count++;
      }

      this.keys = keyBuilder.toArray(new WritableComparable[count]);
      positions = Arrays.copyOf(positions, count);
    } catch (EOFException e) {
      LOG.warn("Unexpected EOF reading " + index +
                            " at entry #" + count + ".  Ignoring.");
    } finally {
	indexClosed = true;
      index.close();
    }
  }

  /** Re-positions the reader before its first key. */
  public synchronized void reset() throws IOException {
    data.get().seek(firstPosition);
  }

  /** Get the key at approximately the middle of the file. Or null if the
   *  file is empty.
   */
  public synchronized WritableComparable midKey() throws IOException {

    readIndex();
    if (count == 0) {
      return null;
    }

    return keys[(count - 1) / 2];
  }

  /** Reads the final key from the file.
   *
   * @param key key to read into
   */
  public synchronized void finalKey(WritableComparable key)
    throws IOException {

    readIndex();                              // make sure index is valid
    if (count > 0) {
      data.get().seek(positions[count-1]);          // skip to last indexed entry
    } else {
      reset();                                // start at the beginning
    }
    while (data.get().next(key)) {}                 // scan to eof
  }

  private long findPosition(WritableComparable key)
    throws IOException {
    readIndex();                                // make sure index is read

    long seekPosition = -1;
    int seekIndex = binarySearch(key);
    if (seekIndex < 0)                        // decode insertion point
      seekIndex = -seekIndex-2;

    if (seekIndex == -1)                      // belongs before first entry
      seekPosition = firstPosition;           // use beginning of file
    else
      seekPosition = positions[seekIndex];    // else use index
    data.get().seek(seekPosition);

    WritableComparable nextKey = comparator.newKey();

    while (data.get().next(nextKey)) {
      int c = comparator.compare(key, nextKey);
      if (c < 0) {                             // at or beyond desired
        return -1;
      } else if (c == 0) {
        return data.get().getPosition();
      }
    }

    return -1;
  }

  private int binarySearch(WritableComparable key) {
    int low = 0;
    int high = count-1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      WritableComparable midVal = keys[mid];
      int cmp = comparator.compare(midVal, key);

      if (cmp < 0)
        low = mid + 1;
      else if (cmp > 0)
        high = mid - 1;
      else
        return mid;                             // key found
    }
    return -(low + 1);                          // key not found.
  }

  /** Return the value for the named key, or null if none exists. */
  public Writable get(WritableComparable key, Writable val)
    throws IOException {
    long position = findPosition(key);
    if (position >= 0) {
      SequenceFile.Reader threadLocalData = data.get();
      threadLocalData.seek(position);
      threadLocalData.getCurrentValue(val);
      return val;
    } else
      return null;
  }

  /** Close the map. */
  public synchronized void close() throws IOException {
    if (!indexClosed) {
      index.close();
    }
    for (SequenceFile.Reader dataFile : allDataFiles) {
      dataFile.close();
    }
  }
}
