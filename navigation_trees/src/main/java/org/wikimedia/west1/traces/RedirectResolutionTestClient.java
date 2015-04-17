package org.wikimedia.west1.traces;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;

public class RedirectResolutionTestClient {

  private static Map<String, String> readRedirects(String lang) throws IOException {
    String file = System.getenv("HOME") + "/wikimedia/trunk/data/redirects/no_date/" + lang
        + "_redirects.tsv.gz";
    Map<String, String> redirects = new HashMap<String, String>();
    InputStream is = new GZIPInputStream(new FileInputStream(file));
    Scanner sc = new Scanner(is, "UTF-8").useDelimiter("\n");
    while (sc.hasNext()) {
      String[] tokens = sc.next().split("\t", 2);
      String src = tokens[0];
      String tgt = tokens[1];
      redirects.put(src, tgt);
    }
    sc.close();
    return redirects;
  }

  /**
   * @param args
   * @throws IOException
   * @throws ClassNotFoundException
   */
  public static void main(String[] args) throws IOException, ClassNotFoundException {
    System.out.print("Reading redirects ...");
    Map<String, String> redirects = readRedirects("pt");
    System.out.println("DONE");
    while (true) {
      for (String key : redirects.keySet()) {
        Socket sock = new Socket("ilws6", 8080);
        ObjectOutputStream outToServer = new ObjectOutputStream(sock.getOutputStream());
        ObjectInputStream inFromServer = new ObjectInputStream(sock.getInputStream());
        outToServer.writeObject("pt");
        outToServer.writeObject(key);
        String s = (String) inFromServer.readObject();
        // System.out.println(s);
        inFromServer.close();
        outToServer.close();
        sock.close();
      }
    }
  }

}
