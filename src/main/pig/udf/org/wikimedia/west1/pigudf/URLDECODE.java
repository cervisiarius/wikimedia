package org.wikimedia.west1.pigudf;

import java.io.IOException;
import java.net.URLDecoder;
import org.apache.hadoop.io.WritableComparable;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;

public class URLDECODE extends EvalFunc<String> {
  public String exec(Tuple input) throws IOException {
    if (input == null || input.size() == 0) {
      return null;
    }
    try {
      String str = (String) input.get(0);
      try {
        return URLDecoder.decode(str, "UTF-8");
      } catch (Exception e) {
        return str;
      }
    } catch(Exception e) {
      throw WrappedIOException.wrap("Caught exception processing input row: ", e);
    }
  }
}
