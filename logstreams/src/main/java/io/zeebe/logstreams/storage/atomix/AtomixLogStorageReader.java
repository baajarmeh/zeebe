/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.logstreams.storage.atomix;

import io.atomix.raft.storage.log.RaftLogReader;
import io.atomix.raft.storage.log.entry.RaftLogEntry;
import io.atomix.raft.zeebe.ZeebeEntry;
import io.atomix.storage.journal.Indexed;
import io.zeebe.logstreams.spi.LogStorage;
import io.zeebe.logstreams.spi.LogStorageReader;
import java.util.Optional;
import org.agrona.DirectBuffer;

public final class AtomixLogStorageReader implements LogStorageReader {
  private final RaftLogReader reader;

  public AtomixLogStorageReader(final RaftLogReader reader) {
    this.reader = reader;
  }

  /**
   * Naive implementation that reads the whole log to check for a {@link ZeebeEntry}. Most of the
   * log should be made of these, so in practice this should be fast enough, however callers should
   * take care when calling this method.
   *
   * <p>The reader will be positioned either at the end of the log, or at the position of the first
   * {@link ZeebeEntry} encountered, such that reading the next entry will return the entry after
   * it.
   *
   * @return true if there are no {@link ZeebeEntry}, false otherwise
   */
  @Override
  public boolean isEmpty() {
    // although seemingly inefficient, the log will contain mostly ZeebeEntry entries and a few
    // InitialEntry, so this should be rather fast in practice
    reader.reset();
    while (reader.hasNext()) {
      if (reader.next().type() == ZeebeEntry.class) {
        return false;
      }
    }
    return true;
  }

  @Override
  public long read(final DirectBuffer readBuffer, final long address) {
    final long index = reader.reset(address);

    if (index > address) {
      return LogStorage.OP_RESULT_INVALID_ADDR;
    }

    if (!reader.hasNext()) {
      return LogStorage.OP_RESULT_NO_DATA;
    }

    final Optional<Indexed<ZeebeEntry>> maybeEntry = findEntry(address);
    if (maybeEntry.isEmpty()) {
      return LogStorage.OP_RESULT_NO_DATA;
    }

    final Indexed<ZeebeEntry> entry = maybeEntry.get();
    final long serializedRecordsLength = wrapEntryData(entry, readBuffer);

    if (serializedRecordsLength < 0) {
      return serializedRecordsLength;
    }

    // for now assume how indexes increase - in the future we should rewrite how we read the
    // logstream to completely ignore addresses entirely
    return entry.index() + 1;
  }

  @Override
  public long readLastBlock(final DirectBuffer readBuffer) {
    reader.seekToAsqn(Long.MAX_VALUE);

    if (reader.hasNext()) {
      final Indexed<RaftLogEntry> entry = reader.next();
      if (entry.type() == ZeebeEntry.class) {
        wrapEntryData(entry.cast(), readBuffer);
        return entry.index() + 1;
      }
    }

    return LogStorage.OP_RESULT_NO_DATA;
  }

  /**
   * Performs binary search over all known Atomix entries to find the entry containing our position.
   *
   * <p>{@inheritDoc}
   */
  @Override
  public long lookUpApproximateAddress(final long position) {
    if (position == Long.MIN_VALUE) {
      return readFirstBlock();
    }

    final long address;
    try {
      address = reader.seekToAsqn(position);
    } catch (final UnsupportedOperationException e) {
      // in case we seek to an unknown position
      return LogStorage.OP_RESULT_INVALID_ADDR;
    }

    if (!reader.hasNext()) {
      return LogStorage.OP_RESULT_INVALID_ADDR;
    }

    return address;
  }

  @Override
  public void close() {
    reader.close();
  }

  private long readFirstBlock() {
    // in the reader being at the end of the log
    return findEntry(0).map(Indexed::index).orElse(LogStorage.OP_RESULT_INVALID_ADDR);
  }

  /**
   * Looks up the entry whose index is either the given index, or the closest lower index.
   *
   * @param index index to seek to
   */
  public Optional<Indexed<ZeebeEntry>> findEntry(final long index) {
    if (reader.getNextIndex() != index) {
      final long nextIndex = reader.reset(index);

      if (nextIndex < index) {
        return Optional.empty();
      }
    }

    while (reader.hasNext()) {
      final var entry = reader.next();
      if (entry.type().equals(ZeebeEntry.class)) {
        return Optional.of(entry.cast());
      }
    }

    return Optional.empty();
  }

  private long wrapEntryData(final Indexed<ZeebeEntry> entry, final DirectBuffer dest) {
    final var data = entry.entry().data();
    final var length = data.remaining();
    dest.wrap(data, data.position(), data.remaining());
    return length;
  }
}
