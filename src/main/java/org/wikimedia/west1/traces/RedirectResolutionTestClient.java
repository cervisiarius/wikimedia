package org.wikimedia.west1.traces;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class RedirectResolutionTestClient {

  /**
   * @param args
   * @throws IOException
   * @throws ClassNotFoundException
   */
  public static void main(String[] args) throws IOException, ClassNotFoundException {
    Socket sock = new Socket("ilws6", 8080);
    ObjectOutputStream outToServer = new ObjectOutputStream(sock.getOutputStream());
    ObjectInputStream inFromServer = new ObjectInputStream(sock.getInputStream());
    outToServer.writeObject("pt");
    outToServer.writeObject("USA");
    System.out.println((String) inFromServer.readObject());
    inFromServer.close();
    outToServer.close();
    sock.close();
  }

}
