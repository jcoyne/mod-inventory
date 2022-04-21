package org.folio.inventory.common.domain;

import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.NonNull;

public class  MultipleRecords <T> {
  public final List<T> records;
  public final Integer totalRecords;

  public MultipleRecords(@NonNull List<T> records) {
    this(records, records.size());
  }

  public MultipleRecords(@NonNull List<T> records, Integer totalRecords) {
    this.records = records;
    this.totalRecords = totalRecords;
  }

  /**
   * Generate a set of distinct values (keys) from the collection of records
   *
   * @param keyMapper function to map a record to a key
   * @param <R> type of key
   * @return a set of keys
   */
  public <R> Set<R> toKeys(Function<T, R> keyMapper) {
    return records.stream()
      .map(keyMapper)
      .collect(Collectors.toSet());
  }
}
