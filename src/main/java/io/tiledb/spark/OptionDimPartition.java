package io.tiledb.spark;

import java.util.Optional;

public class OptionDimPartition {

  private String name;
  private Integer idx;
  private Integer npartitions = 1;

  public OptionDimPartition(String dimNameOrIdx, String value) throws IllegalArgumentException {
    String[] splitDimId = dimNameOrIdx.split("\\.");
    System.out.println(splitDimId.length);
    if (splitDimId.length != 2) {
      throw new IllegalArgumentException(
          "Invalid dimension option key, expected 'dim.idx` or 'dim.name' got: " + dimNameOrIdx);
    }
    try {
      idx = Integer.parseInt(splitDimId[1]);
      if (idx < 0) {
        throw new IllegalArgumentException(
            "Invalid dimension N partition(s) value, must be a positive integer: " + value);
      }
    } catch (NumberFormatException ex) {
      name = splitDimId[1];
    }
    if (!value.isEmpty()) {
      try {
        npartitions = Integer.parseInt(value);
        if (npartitions < 1) {
          throw new IllegalArgumentException(
              "Invalid dimension N partition(s) value, must be >= 1: " + value);
        }
      } catch (NumberFormatException ex) {
        throw new IllegalArgumentException(
            "Invalid dimension N partition(s) value, must be >= 1: " + value);
      }
    }
  }

  public Optional<Integer> getDimIdx() {
    if (idx == null) {
      return Optional.empty();
    }
    return Optional.of(idx);
  }

  public Optional<String> getDimName() {
    if (name == null) {
      return Optional.empty();
    }
    return Optional.of(name);
  }

  public Integer getNPartitions() {
    return npartitions;
  }
}
