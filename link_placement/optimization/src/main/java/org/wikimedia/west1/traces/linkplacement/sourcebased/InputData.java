package org.wikimedia.west1.traces.linkplacement.sourcebased;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class InputData implements Serializable {

  private static final long serialVersionUID = 100584L;

  // Map from candidate "src > tgt" to map of all values.
  public Map<String, Map<String, Double>> map = new HashMap<String, Map<String, Double>>();

  public void serialize(String fileName) throws IOException {
    ObjectOutput out = new ObjectOutputStream(new FileOutputStream(fileName));
    out.writeObject(this);
    out.close();
  }

  public static InputData deserialize(String fileName) throws IOException, ClassNotFoundException {
    ObjectInputStream in = new ObjectInputStream(new FileInputStream(new File(fileName)));
    InputData object = (InputData) in.readObject();
    in.close();
    return object;
  }

}
