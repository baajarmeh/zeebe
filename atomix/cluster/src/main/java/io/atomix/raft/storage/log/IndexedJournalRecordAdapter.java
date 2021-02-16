/*
 * Copyright © 2020 camunda services GmbH (info@camunda.com)
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
package io.atomix.raft.storage.log;

import static io.zeebe.journal.file.SegmentedJournal.ASQN_IGNORE;

import io.atomix.raft.storage.log.entry.RaftLogEntry;
import io.atomix.raft.zeebe.ZeebeEntry;
import io.atomix.storage.journal.Indexed;
import io.zeebe.journal.JournalRecord;
import org.agrona.DirectBuffer;

public final class IndexedJournalRecordAdapter implements JournalRecord {
  private final Indexed<RaftLogEntry> indexedEntry;
  private final DirectBuffer data;

  public IndexedJournalRecordAdapter(
      final Indexed<RaftLogEntry> indexedEntry, final DirectBuffer data) {
    this.indexedEntry = indexedEntry;
    this.data = data;
  }

  @Override
  public long index() {
    return indexedEntry.index();
  }

  @Override
  public long asqn() {
    if (indexedEntry.type() == ZeebeEntry.class) {
      final Indexed<ZeebeEntry> asqnEntry = indexedEntry.cast();
      return asqnEntry.entry().lowestPosition();
    }

    return ASQN_IGNORE;
  }

  @Override
  public int checksum() {
    return (int) indexedEntry.checksum();
  }

  @Override
  public DirectBuffer data() {
    return data;
  }
}
