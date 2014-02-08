import java.io.*;
import java.net.*;

public class BrokerClient
{
  public static void main(String[] args)
         throws IOException, ClassNotFoundException
  {
    String localExchange = null;
    NamingService ns = null;
    ConnectionWrapper cw_broker = null;
    BrokerPacket packetToServer = null;
    BrokerPacket packetFromServer = null;

    try
    {
      // Setup name server connection
      if(args.length == 2 )
      {
        ns = new NamingService( args[0], Integer.parseInt(args[1]) );
      }
      else
      {
        System.err.println("ERROR: Invalid arguments!");
        System.exit(-1);
      }
    }
    catch (UnknownHostException e)
    {
      System.err.println("ERROR: Don't know where to connect for the naming server!!");
      System.exit(1);
    }
    catch (IOException e)
    {
      System.err.println("ERROR: Couldn't get I/O for the naming server connection.");
      System.exit(1);
    }

    BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));
    String userInput;

    System.out.print("Enter command, symbol or quit for exit:\nClient> ");
    while ( (userInput = stdIn.readLine() ) != null && (! userInput.equalsIgnoreCase("quit") ) )
    {
      if ( userInput.startsWith("local") )
      {
        if ( userInput.contains(" ") )
        {
          String delims = "[ ]";
          String[] tokens = userInput.split(delims);
          int arrySize = tokens.length;
          if (arrySize == 2)
          {
            String newLocalExchange = tokens[1];
            if ( newLocalExchange.equalsIgnoreCase(localExchange) )
            {
              System.out.println("Current local exchage is already " + localExchange);
            }
            else
            {
              localExchange = newLocalExchange;
              BrokerLocation bl = ns.lookupNameServer(newLocalExchange);
              if (bl != null)
              {
                // If this is existing broker connection, close it.
                if (cw_broker != null)
                {
                  System.out.println("Closing existing connection with the broker.");
                  cw_broker.closeConnection_clientSide();
                }
                try
                {
                  cw_broker = new ConnectionWrapper(bl.broker_host, bl.broker_port);
                  System.out.println(newLocalExchange + " as local.");
                }
                catch (UnknownHostException e)
                {
                  System.err.println("ERROR: Don't know where to connect for broker!!");
                  System.exit(1);
                }
                catch (IOException e)
                {
                  System.err.println("ERROR: Couldn't get I/O for the connection for broker.");
                  System.exit(1);
                }
                catch (ClassNotFoundException e)
                {
                  System.err.println("ERROR: OnlineBrokerHandlerThread: handleClient: ClassNotFoundException!");
                  e.printStackTrace();
                  System.exit(-1);
                }
              }
              else
              {
                System.out.println("The local broker is not registered on the name server.");
              }
            }
          }
          else
          {
            System.out.println("Invalid command format!\nlocal <exchange>");
          }
        }
        else
        {
          System.out.println("Invalid command format!\nlocal <exchange>\n");
        }
      }
      // If not local command
      else
      {
        if (cw_broker != null)
        {
          packetToServer = new BrokerPacket();
          packetToServer.type = BrokerPacket.BROKER_REQUEST;
          packetToServer.symbol = userInput;
          cw_broker.sendPacket(packetToServer);

          packetFromServer = cw_broker.getPacket();
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
        }
        else
        {
          System.out.println("local broker is not set!\ncommand: local <exchange>");
        }
      }

      // re-print console prompt
      System.out.print("Client> ");
    }

    stdIn.close();
    cw_broker.closeConnection_clientSide();
    ns.closeNameServerConnection();
  }

}
