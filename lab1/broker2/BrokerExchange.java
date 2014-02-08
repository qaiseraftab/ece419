import java.io.*;
import java.net.*;

public class BrokerExchange {
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

    System.out.print("Enter command or quit for exit:\nExchange> ");
    while ( (userInput = stdIn.readLine() ) != null && (! userInput.equalsIgnoreCase("quit") ) )
    {
      packetToServer = new BrokerPacket();
      String delims = "[ ]";
      String[] tokens = userInput.split(delims);

      int arrySize = tokens.length;
      if ( tokens[0].equalsIgnoreCase("add") && arrySize == 2 )
      {
        packetToServer.type = BrokerPacket.EXCHANGE_ADD;
        packetToServer.symbol = tokens[1];
      }
      else if ( tokens[0].equalsIgnoreCase("remove") && arrySize == 2 )
      {
        packetToServer.type = BrokerPacket.EXCHANGE_REMOVE;
        packetToServer.symbol = tokens[1];
      }
      else if ( tokens[0].equalsIgnoreCase("update") && arrySize == 3 )
      {
        packetToServer.type = BrokerPacket.EXCHANGE_UPDATE;
        packetToServer.symbol = tokens[1];
        packetToServer.quote = Long.parseLong(tokens[2]);
      }
      else
      {
        System.out.print("Invalid command format\n[add, remove, update] <symbol>\nExchange> ");
        continue;
      }

      out.writeObject(packetToServer);

      packetFromServer = (BrokerPacket) in.readObject();

      if (packetFromServer.type == BrokerPacket.EXCHANGE_REPLY)
      {
        if (packetToServer.type == BrokerPacket.EXCHANGE_ADD)
        {
          System.out.println(tokens[1] + " added.");
        }
        else if (packetToServer.type == BrokerPacket.EXCHANGE_REMOVE)
        {
          System.out.println(tokens[1] + " removed.");
        }
        else if (packetToServer.type == BrokerPacket.EXCHANGE_UPDATE)
        {
          System.out.println(tokens[1] + " updated to " + tokens[2] + ".");
        }
        else
        {
          // Must not reach this code!!
          System.out.println("packet to server type is corrupted.");
        }
      }
      else if (packetFromServer.type == BrokerPacket.ERROR_INVALID_SYMBOL)
      {
        System.out.println(tokens[1] + " invalid.");
      }
      else if (packetFromServer.type == BrokerPacket.ERROR_OUT_OF_RANGE)
      {
        System.out.println(tokens[1] + " out of range.");
      }
      else if (packetFromServer.type == BrokerPacket.ERROR_SYMBOL_EXISTS)
      {
        System.out.println(tokens[1] + " exists.");
      }
      else
      {
        System.out.println("BrokerPacket from the server has incorrect type: " + packetFromServer.type);
      }

      // re-print console prompt
      System.out.print("Exchange> ");
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
