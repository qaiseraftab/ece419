import java.net.*;
import java.io.*;

public class OnlineBroker
{

  public static void main(String[] args) throws IOException
  {
    ServerSocket serverSocket = null;
    String exchange = null;
    NamingService ns = null;
    int broker_port = 0;
    boolean listening = true;

    try
    {
      if(args.length == 4)
      {
        exchange = args[3];
        broker_port = Integer.parseInt(args[2]);
        serverSocket = new ServerSocket(broker_port);
        ns = new NamingService( args[0], Integer.parseInt(args[1]) );

        // If registration to the name server fails
        if ( ! ns.registerToNameServer( exchange, InetAddress.getLocalHost().getHostAddress(), broker_port ) )
        {
            System.err.println("ERROR: registration to the name server failed!");
            System.exit(-1);
        }
      } 
      else
      {
        System.err.println("ERROR: Invalid arguments!");
        System.exit(-1);
      }
    } 
    catch (IOException e)
    {
      System.err.println("ERROR: Could not listen on port!");
      System.exit(-1);
    }
    catch (ClassNotFoundException cnfe)
    {
      System.err.println("ERROR: Could not find a class!");
      System.exit(-1);
    }

    while (listening)
    {
      new OnlineBrokerHandlerThread( serverSocket.accept(), exchange, ns ).start();
    }

    serverSocket.close();
  }

}
