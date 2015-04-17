package org.wikimedia.west1.traces;

//With ideas from http://tutorials.jenkov.com/java-multithreaded-servers/thread-pooled-server.html

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPInputStream;

public class RedirectResolutionServer implements Runnable {

  private int serverPort;
  private ServerSocket server;
  protected ExecutorService threadPool;
  protected boolean isStopped = false;
  protected Thread runningThread = null;

  // The redirects; they're read from file when this Mapper instance is created.
  private Map<String, Map<String, String>> redirects = new HashMap<String, Map<String, String>>();

  private void readRedirectsFromInputStream(String lang, InputStream is) {
    Scanner sc = new Scanner(is, "UTF-8").useDelimiter("\n");
    Map<String, String> redirectsForLang = new HashMap<String, String>();
    redirects.put(lang, redirectsForLang);
    while (sc.hasNext()) {
      String[] tokens = sc.next().split("\t", 2);
      String src = tokens[0];
      String tgt = tokens[1];
      redirectsForLang.put(src, tgt);
    }
    sc.close();
  }

  private void readAllRedirects(String[] languages) {
    for (String lang : languages) {
      String file = System.getenv("HOME") + "/wikimedia/trunk/data/redirects/no_date/" + lang
          + "_redirects.tsv.gz";
      try {
        InputStream is = new GZIPInputStream(new FileInputStream(file));
        readRedirectsFromInputStream(lang, is);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public class WorkerRunnable implements Runnable {

    protected Socket clientSocket = null;

    public WorkerRunnable(Socket clientSocket) {
      this.clientSocket = clientSocket;
    }

    @Override
    public void run() {
      try {
        serveClient(clientSocket);
        clientSocket.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  // languageString is of the form "de|pt|en".
  public RedirectResolutionServer(String languageString, int serverPort, int numThreads)
      throws Exception {
    this.serverPort = serverPort;
    this.threadPool = Executors.newFixedThreadPool(numThreads);
    String[] languages = languageString.split("\\|");
    System.out.print("Reading redirects ...");
    long before = System.currentTimeMillis();
    readAllRedirects(languages);
    System.out.println("DONE" + ((System.currentTimeMillis()-before)/1000));
  }

  private void openServerSocket() {
    try {
      server = new ServerSocket(serverPort);
    } catch (IOException e) {
      throw new RuntimeException("Cannot open port " + serverPort, e);
    }
  }

  @Override
  public void run() {
    synchronized (this) {
      this.runningThread = Thread.currentThread();
    }
    openServerSocket();
    int i = 0;
    System.err.println("TCP server waiting for client on port " + serverPort);
    while (!isStopped) {
      Socket connected = null;
      try {
        connected = server.accept();
      } catch (IOException e) {
        if (isStopped) {
          System.err.println("Server Stopped.");
          return;
        }
        throw new RuntimeException("Error accepting client connection", e);
      }
      if (++i % 1e5 == 0) {
        System.err.print(".");
      }
      this.threadPool.execute(new WorkerRunnable(connected));
    }
    threadPool.shutdown();
    System.err.println("Server Stopped.");
  }

  public synchronized void stop() {
    isStopped = true;
    try {
      server.close();
    } catch (IOException e) {
      throw new RuntimeException("Error closing server", e);
    }
  }

  private void serveClient(Socket sock) throws IOException {
    ObjectInputStream inFromClient = new ObjectInputStream(sock.getInputStream());
    ObjectOutputStream outToClient = new ObjectOutputStream(sock.getOutputStream());
    try {
      String lang = (String) inFromClient.readObject();
      String article = (String) inFromClient.readObject();
      outToClient.writeObject(redirects.get(lang).get(article));
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      outToClient.close();
      inFromClient.close();
    }
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    //String languageString = args[0];
    //int numThreads = Integer.parseInt(args[1]);
    String languageString = "en";
    int numThreads = 10;
    RedirectResolutionServer server = new RedirectResolutionServer(languageString, 8080, numThreads);
    new Thread(server).start();
    Scanner console = new Scanner(System.in);
    while (console.hasNextLine()) {
      if (console.next().equals("stop")) {
        System.err.println("Stopping Server");
        server.stop();
        return;
      }
    }
  }

}
