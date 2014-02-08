import java.io.*;
import java.net.*;

public class BrokerExchange {
  public static void main(String[] args) 
         throws IOException, ClassNotFoundException
  {

    String exchange = null;
    NamingService ns = null;
    ConnectionWrapper cw_broker = null;
    BrokerPacket packetToServer = null;
    BrokerPacket packetFromServer = null;
    
    try
    {
      if(args.length == 3)
      {
        exchange = args[2];
        ns = new NamingService( args[0], Integer.parseInt(args[1]) );
        BrokerLocation bl = ns.lookupNameServer(exchange);

        if (bl != null)
        {
          cw_broker = new ConnectionWrapper(bl.broker_host, bl.broker_port);
        }
        else
        {
          System.err.println("ERROR: BrokerExchange: " + exchange + " broker is not registered on the naming server!");
          System.exit(1);
        }
      }
      else
      {
        System.err.println("ERROR: Invalid arguments!");
        System.exit(-1);
      }

    }
    catch (UnknownHostException e)
    {
      System.err.println("ERROR: Don't know where to connect!!");
      e.printStackTrace();
      System.exit(1);
    }
    catch (IOException e)
    {
      System.err.println("ERROR: Couldn't get I/O for the connection.");
      e.printStackTrace();
      System.exit(1);
    }
    catch (ClassNotFoundException e)
    {
      System.err.println("ERROR: BrokerExchange: ClassNotFoundException!");
      e.printStackTrace();
      System.exit(-1);
    }

    BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));
    String userInput;

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
        System.out.print("Invalid command format!\n[add, remove, update] <symbol>\nExchange> ");
        continue;
      }

      cw_broker.sendPacket(packetToServer);
      packetFromServer = cw_broker.getPacket();

      switch(packetFromServer.type)
      {
        case BrokerPacket.EXCHANGE_REPLY:
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
          break;

        case BrokerPacket.ERROR_INVALID_SYMBOL:
          System.out.println(tokens[1] + " invalid.");
          break;
      
        case BrokerPacket.ERROR_OUT_OF_RANGE:
          System.out.println(tokens[1] + " out of range.");
          break;

        case BrokerPacket.ERROR_SYMBOL_EXISTS:
          System.out.println(tokens[1] + " exists.");
          break;

        default:
          System.out.println("BrokerPacket from the server has incorrect type: " + packetFromServer.type);
      }

      // re-print console prompt
      System.out.print("Exchange> ");
    }

    stdIn.close();
    cw_broker.closeConnection_clientSide();
    ns.closeNameServerConnection();
  }
}
