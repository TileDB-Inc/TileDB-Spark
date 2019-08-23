package io.tiledb.spark;

import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;

public class OptionDimPartitionTest {

  @Test
  public void testValidDimName() throws Exception {
    OptionDimPartition partition = new OptionDimPartition("dim.foo", "10");
    Optional<String> name = partition.getDimName();
    Assert.assertTrue(name.isPresent());
    Assert.assertEquals("foo", name.get());
    Integer val = partition.getNPartitions();
    Assert.assertEquals(10, (int) val);
  }

  @Test
  public void testValidDimIdx() throws Exception {
    OptionDimPartition partition = new OptionDimPartition("dim.0", "10");
    Optional<String> name = partition.getDimName();
    Assert.assertFalse(name.isPresent());
    Optional<Integer> idx = partition.getDimIdx();
    Assert.assertTrue(idx.isPresent());
    Assert.assertEquals(0, (int) idx.get());
    Integer val = partition.getNPartitions();
    Assert.assertEquals(10, (int) val);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInValidDimIdx() throws Exception {
    OptionDimPartition partition = new OptionDimPartition("dim.-1", "0");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInValidDimName() throws Exception {
    OptionDimPartition partition = new OptionDimPartition("dim.", "0");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidPartitionValue() throws Exception {
    OptionDimPartition partition = new OptionDimPartition("dim.foo", "foo");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidPartitionNumber() throws Exception {
    OptionDimPartition partition = new OptionDimPartition("dim.foo", "-1");
  }
}
