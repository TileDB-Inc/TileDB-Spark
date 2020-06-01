package io.tiledb.spark;

import io.tiledb.java.api.Pair;
import java.util.ArrayList;
import java.util.List;

public class StringPartitioner {
  private int MIN_ASCII_CHAR = 0;
  private int MAX_ASCII_CHAR = 127;

  private int minChar;
  private int maxChar;

  public StringPartitioner() {
    minChar = MIN_ASCII_CHAR;
    maxChar = MAX_ASCII_CHAR;
  }

  public StringPartitioner(int minChar, int maxChar) {
    this.minChar = minChar;
    this.maxChar = maxChar;
  }

  /**
   * Computes the next possible string by incrementing the least possible character. The next string
   * is limited to the input string length. For example, if the minimum character provided is 'z',
   * then a string like `zzz` cannot be further incremented and it will be returned as it is. On the
   * other hand, the next string of `aaz` will be `aba`, the next of `a` `b` and so on.
   *
   * <p>The example above is assumes the a-z alphabet for simplicity. In practice, the next string
   * depends on the min/max chars defined by the user in the StringPartitioner constructor. The
   * default values are the min and max ASCII characters.
   *
   * @param str The input string
   * @return The next string
   */
  public String nextStr(String str) {
    int strLen = str.length();

    char[] charArray = str.toCharArray();

    int currentCharIdx = strLen - 1;

    while (currentCharIdx > 0 && charArray[currentCharIdx] == maxChar) {
      charArray[currentCharIdx] = (char) minChar;
      --currentCharIdx;
    }

    if (charArray[currentCharIdx] < maxChar) charArray[currentCharIdx]++;

    return new String(charArray);
  }

  /**
   * Computes a the n-th next string
   *
   * @param str The input string
   * @param n The n value
   * @return The n-th next string
   */
  public String nextStr(String str, long n) {
    for (int i = 0; i < n; ++i) str = this.nextStr(str);

    return str;
  }

  /**
   * Returns the distance between two strings. By distance, we mean the maximum possible strings
   * that can be created between a1 and a2 in lexicographical order.
   *
   * @param a1 The left bound
   * @param a2 The right bound
   * @return The number of possible strings
   */
  public int distance(String a1, String a2) {
    String tmp = a1;
    int dist = 0;
    while (tmp.compareTo(a2) < 0) {
      tmp = this.nextStr(tmp);
      ++dist;
    }

    return dist;
  }

  /**
   * Adds extra chars to the input string
   *
   * @param str The input string
   * @param c The char to be added to the end of the string
   * @param n The occurrences of c to be added
   * @return The new string
   */
  public static String addExtraChars(String str, char c, int n) {
    char[] newStr = new char[str.length() + n];
    int idx = 0;

    for (char character : str.toCharArray()) newStr[idx++] = character;

    while (idx < str.length() + n) {
      newStr[idx++] = c;
    }

    return new String(newStr);
  }

  /**
   * Splits a String range into equi-width sub-ranges
   *
   * @param start The left bound
   * @param end The right bound
   * @param partitions The number of partitions
   * @return A list of pairs with the sub-range bounds
   */
  public List<Pair<String, String>> split(String start, String end, int partitions) {
    String fixedStart = start;
    long width = this.distance(fixedStart, end);

    while (width / partitions < 1) {
      // In this case we need to add extra characters to the left string
      fixedStart = addExtraChars(fixedStart, (char) minChar, 1);
      width = this.distance(fixedStart, end);
    }

    long partitionWidth = (width / partitions);

    String tmpStart = fixedStart;
    String tmpEnd;

    List<Pair<String, String>> list = new ArrayList<>();

    for (int i = 0; i < partitions; ++i) {
      if (i == partitions - 1) tmpEnd = end;
      else tmpEnd = nextStr(tmpStart, partitionWidth);

      if (i == 0) tmpStart = start;

      if (tmpEnd.compareTo(end) > 0) {
        tmpEnd = end;
        list.add(new Pair(new String(tmpStart.toCharArray()), new String(tmpEnd.toCharArray())));
        break;
      }

      list.add(new Pair(new String(tmpStart.toCharArray()), new String(tmpEnd.toCharArray())));
      tmpStart = nextStr(tmpEnd, 1);
    }

    return list;
  }
}
