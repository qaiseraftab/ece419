import java.io.*;
import java.net.*;

public class BrokerClient {
  public static void main(String[] args) throws IOException,
      ClassNotFoundException {

    Socket echoSocket = null;
    ObjectOutputStream out = null;
    ObjectInputStream in = null;

    try
    {
      /* variables for hostname/port */
      String hostname = "localhost";
      int port = 4444;
      
      if(args.length == 2 )
      {
        hostname = args[0];
        port = Integer.parseInt(args[1]);
      }
      else
      {
        System.err.println("ERROR: Invalid arguments!");
        System.exit(-1);
      }
      echoSocket = new Socket(hostname, port);

      out = new ObjectOutputStream(echoSocket.getOutputStream());
      in = new ObjectInputStream(echoSocket.getInputStream());

    }
    catch (UnknownHostException e)
    {
      System.err.println("ERROR: Don't know where to connect!!");
      System.exit(1);
    }
    catch (IOException e)
    {
      System.err.println("ERROR: Couldn't get I/O for the connection.");
      System.exit(1);
    }

    BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));
    String userInput;

    BrokerPacket packetToServer = null;
    BrokerPacket packetFromServer = null;

    System.out.print("Enter symbol or quit for exit:\nClient> ");
    while ( (userInput = stdIn.readLine() ) != null && (! userInput.equalsIgnoreCase("quit") ) )
    {
      // Make a new broker request packet
      packetToServer = new BrokerPacket();
      packetToServer.type = BrokerPacket.BROKER_REQUEST;
      packetToServer.symbol = userInput;
      out.writeObject(packetToServer);

      // print quote obtained from server
      packetFromServer = (BrokerPacket) in.readObject();

      if (packetFromServer.type == BrokerPacket.BROKER_QUOTE)
      {
        if (packetFromServer.quote != null)
        {
          System.out.println("Quote from broker: " + packetFromServer.quote);
        }
        else
        {
          System.out.println("No Quote obtained from the broker!");
        }
      }
      else if (packetFromServer.type == BrokerPacket.ERROR_INVALID_SYMBOL)
      {
        System.out.println(userInput + " invalid.");
      }
      else
      {
        System.out.println("BrokerPacket from the server has incorrect type: " + packetFromServer.type);
      }

      // re-print console prompt
      System.out.print("Client> ");
    }

    // Upon user's exit input, tell server that i'm quitting
    packetToServer = new BrokerPacket();
    packetToServer.type = BrokerPacket.BROKER_BYE;
    packetToServer.symbol = "Bye!";
    out.writeObject(packetToServer);

    out.close();
    in.close();
    stdIn.close();
    echoSocket.close();
  }
}
