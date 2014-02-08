import java.net.*;
import java.io.*;

public class OnlineBrokerHandlerThread extends Thread {
  private String exchange = null;
  private NamingService ns = null;
  private ConnectionWrapper cw_client = null;
  private ConnectionWrapper cw_otherBroker = null;
  private BrokerPacket packetFromClient = null;
  private BrokerPacket packetToClient = null;

  private static final int ERROR_HANDLE_EXCHANGE_QUOTE_DNE = 1;

  private static final int QUOTE_LOWERBOUND = 1;
  private static final int QUOTE_UPPERBOUND = 300;

  public OnlineBrokerHandlerThread(Socket socket, String exchange, NamingService ns)
  {
    super("OnlineBrokerHandlerThread");
    this.exchange = exchange;
    this.ns = ns;
    try
    {
      this.cw_client = new ConnectionWrapper(socket);
      System.out.println("Created new Thread to handle client(" + InetAddress.getLocalHost().getHostAddress() + ")");
    }
    catch (IOException e)
    {
      System.err.println("ERROR: OnlineBrokerHandlerThread: constructor: Could not listen on port!");
      e.printStackTrace();
      System.exit(-1);
    }
    catch (ClassNotFoundException e)
    {
      System.err.println("ERROR: OnlineBrokerHandlerThread: constructor: ClassNotFoundException!");
      e.printStackTrace();
      System.exit(-1);
    }
  }

  public void run()
  {
    boolean gotByePacket = false;
    try
    {
      while ( ( packetFromClient = cw_client.getPacket() ) != null )
      {
        if (packetFromClient.type > BrokerPacket.BROKER_NULL && packetFromClient.type < BrokerPacket.BROKER_BYE)
        {
          handleClient();

          // Send reply back to client 
          cw_client.sendPacket(packetToClient);
        }

        // EXCHANGE_ADD = 201  and  EXCHANGE_REPLY = 205
        else if (packetFromClient.type > 200 && packetFromClient.type < 205)
        {
          handleExchange();
          cw_client.sendPacket(packetToClient);
        }
        
        // Sending an BROKER_NULL || BROKER_BYE means quit
        else if (packetFromClient.type == BrokerPacket.BROKER_NULL || packetFromClient.type == BrokerPacket.BROKER_BYE)
        {
          gotByePacket = true;
          System.out.println("A client(" + InetAddress.getLocalHost().getHostAddress() + ") has disconnected.");
          break;
        }
        
        // If code comes here, there is an error in the packet
        else
        {
          System.err.println("ERROR: Invalid BROKER_* packet!!");
          System.exit(-1);
        }
      }
      
      // Cleanup when client exits
      if (cw_otherBroker != null)
      {
        cw_otherBroker.closeConnection_clientSide();
      }
      cw_client.closeConnection_serverSide();
    }
    catch (IOException e)
    {
      if(! gotByePacket)
        e.printStackTrace();
    }
  }

  private void handleClient()
  {
    packetToClient = new BrokerPacket();
    String symbolRequested = packetFromClient.symbol;
    if (symbolRequested == null)
    {
      System.out.println("ERROR: OnlineBrokerHandlerThread: handleClient: symbol of packet from client is null!");
      return;
    }
    Long quoteVal = 0L;

    switch (packetFromClient.type)
    {
      // BROKER_REQUEST is always coming from the end client
      case BrokerPacket.BROKER_REQUEST:
        System.out.println("From a client: " + symbolRequested);
        quoteVal = quote_get();
        // If the quote value is found locally
        if (quoteVal != -1)
        {
          packetToClient.type = BrokerPacket.BROKER_QUOTE;
          packetToClient.quote = quoteVal;
          return;
        }
        try
        {
          // If there is no connection setup with the other broker, setup now
          if (cw_otherBroker == null)
          {
            BrokerLocation bl = ns.lookupNameServer( otherExchangeName() );
            // If other exchange is not registered to the name server
            if (bl == null)
            {
              packetToClient.type = BrokerPacket.ERROR_INVALID_SYMBOL;
              return;
            }
            cw_otherBroker = new ConnectionWrapper(bl.broker_host, bl.broker_port);
          }
          BrokerPacket packet_broker = new BrokerPacket();
          packet_broker.type = BrokerPacket.BROKER_FORWARD;
          packet_broker.symbol = packetFromClient.symbol;
          cw_otherBroker.sendPacket(packet_broker);

          if ( (packet_broker = cw_otherBroker.getPacket()) != null )
          {
            packetToClient.type = packet_broker.type;
            if (packet_broker.type == BrokerPacket.BROKER_QUOTE)
            {
              packetToClient.quote = packet_broker.quote;
            }
            else if (packet_broker.type != BrokerPacket.ERROR_INVALID_SYMBOL)
            {
              System.out.println("ERROR: OnlineBrokerHandlerThread: handleClient: invalid packet type arrived from the other broker!");
            }
          }
        }
        catch (IOException e)
        {
          System.err.println("ERROR: OnlineBrokerHandlerThread: handleClient: IOException!");
          e.printStackTrace();
          System.exit(-1);
        }
        catch (ClassNotFoundException e)
        {
          System.err.println("ERROR: OnlineBrokerHandlerThread: handleClient: ClassNotFoundException!");
          e.printStackTrace();
          System.exit(-1);
        }

        break;

      case BrokerPacket.BROKER_FORWARD:
        System.out.println("Forwareded: " + symbolRequested);
        quoteVal = quote_get();
        if (quoteVal != -1)
        {
          packetToClient.type = BrokerPacket.BROKER_QUOTE;
          packetToClient.quote = quoteVal;
          return;
        }
        packetToClient.type = BrokerPacket.ERROR_INVALID_SYMBOL;
        break;

      default:
        System.out.println("ERROR: handleClient: wrong packetFromClient type!");
        System.out.println("type: " + packetFromClient.type);
    }

    return;
  }


  private void handleExchange()
  {
    if (packetFromClient.symbol != null)
    {
      packetToClient = new BrokerPacket();
      Long quoteVal;
      switch (packetFromClient.type)
      {
        case BrokerPacket.EXCHANGE_ADD:
          System.out.println("From a exchange: add " + packetFromClient.symbol);

          // Lookup for existing quote
          quoteVal = quote_get();

          // If symbol match is found
          if (quoteVal != -1)
          {
            System.out.println("Symbol (" + packetFromClient.symbol + ") already exists.");
            packetToClient.type = BrokerPacket.ERROR_SYMBOL_EXISTS;
            return;
          }
          symbol_add();
          break;

        case BrokerPacket.EXCHANGE_REMOVE:
          System.out.println("From a exchange: remove " + packetFromClient.symbol);

          if ( symbol_remove() != 0 )
          {
            packetToClient.type = BrokerPacket.ERROR_INVALID_SYMBOL;
            return;
          }
          break;

        case BrokerPacket.EXCHANGE_UPDATE:
          quoteVal = packetFromClient.quote;
          System.out.println("From a exchange: update " + packetFromClient.symbol + " " + quoteVal);

          if (quoteVal < QUOTE_LOWERBOUND || quoteVal > QUOTE_UPPERBOUND)
          {
            packetToClient.type = BrokerPacket.ERROR_OUT_OF_RANGE;
            return;
          }
          if ( symbol_update() != 0 )
          {
            packetToClient.type = BrokerPacket.ERROR_INVALID_SYMBOL;
            return;
          }
          break;

        default:
          System.out.println("ERROR: handleExchange: wrong packetFromClient type!");
          System.out.println("type: " + packetFromClient.type);
          return;
       }
    }
    else
    {
      System.out.println("ERROR: handleExchange: packetFromClient has NULL symbol!");
      return;
    }

    packetToClient.type = BrokerPacket.EXCHANGE_REPLY;
    return;
  }


  private Long quote_get()
  {
    BufferedReader br = null;
    Long quoteVal = -1L;
    try
    {
      String sCurrentLine;
      String delims = "[ ]";
      br = new BufferedReader( new FileReader(exchange) );
 
      while ( (sCurrentLine = br.readLine() ) != null )
      {
        if ( sCurrentLine.contains(" ") )
        {
          // Tokenize the line with space delimeter and check for a match
          String[] tokens = sCurrentLine.split(delims);
          if ( tokens[0].equalsIgnoreCase(packetFromClient.symbol) )
          {
            quoteVal = Long.parseLong(tokens[1]);
            break;
          }
        }
      }
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
    try
    {
      if (br != null)
      {
        br.close();
      }

      return quoteVal;
    }
    catch (IOException ex)
    {
      ex.printStackTrace();
    }

    return quoteVal;
  }

  private void symbol_add()
  {
    try
    {
      // Append to the file
      BufferedWriter bw = new BufferedWriter( new FileWriter(exchange, true) );

      // Write to the file: set quote to "-1" for uninitialized value
      bw.write(packetFromClient.symbol + " 0\n");
      bw.close();
    }
    catch (IOException ex)
    {
      ex.printStackTrace();
    }
  }

  private int symbol_remove()
  {
    BufferedReader br = null;
    BufferedWriter bw = null;
    int returnVal = ERROR_HANDLE_EXCHANGE_QUOTE_DNE;

    try
    {
      String sCurrentLine;
      String delims = "[ ]";
      br = new BufferedReader( new FileReader(exchange) );
      bw = new BufferedWriter( new FileWriter(exchange + "Temp") );
 
      while ( (sCurrentLine = br.readLine() ) != null )
      {
        if ( sCurrentLine.contains(" ") )
        {
          // Tokenize the line with space delimeter and check for a match
          String[] tokens = sCurrentLine.split(delims);
          if (tokens.length == 2)
          {
            if (! tokens[0].equalsIgnoreCase(packetFromClient.symbol) )
            {
              bw.write(sCurrentLine + "\n");
            }
            else
            {
              returnVal = 0;
            }
          }
          else
          {
            System.out.println("ERROR: OnlineBrokerHandlerThread: quote_get: file format is wrong!");
            break;
          }
        }
        else
        {
          System.out.println("ERROR: OnlineBrokerHandlerThread: quote_get: file format is wrong!");
          break;
        }
      }
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
    try
    {
      if (br != null && bw != null)
      {
        br.close();
        bw.close();
      }
      File origFile = new File(exchange);
      File tempFile = new File(exchange + "Temp");
 
      // If the symbol does not exists in the file, remove the 
      // temporary file and return.
      if (returnVal == ERROR_HANDLE_EXCHANGE_QUOTE_DNE)
      {
        if (! tempFile.delete() )
        {
          System.out.println("ERROR: symbol_remove: Could not delete temp file!");
        }
        return returnVal;
      }

      if(! origFile.delete() )
      {
        System.out.println("ERROR: symbol_remove: Could not delete input file!");
      }
      if(! tempFile.renameTo(origFile) )
      {
        System.out.println("ERROR: symbol_remove: Could not rename output file!");
      }

      return returnVal;
    }
    catch (IOException ex)
    {
      ex.printStackTrace();
    }

    return returnVal;
  }

  private int symbol_update()
  {
    BufferedReader br = null;
    BufferedWriter bw = null;
    int returnVal = ERROR_HANDLE_EXCHANGE_QUOTE_DNE;
    String symbol = packetFromClient.symbol;

    try
    {
      String sCurrentLine;
      String delims = "[ ]";
      br = new BufferedReader( new FileReader(exchange) );
      bw = new BufferedWriter( new FileWriter(exchange + "Temp") );
 
      while ( (sCurrentLine = br.readLine() ) != null )
      {
        if ( sCurrentLine.contains(" ") )
        {
          // Tokenize the line with space delimeter and check for a match
          String[] tokens = sCurrentLine.split(delims);
          if (tokens.length == 2)
          {
            if (! tokens[0].equalsIgnoreCase(packetFromClient.symbol) )
            {
              bw.write(sCurrentLine + "\n");
            }
            else
            {
              returnVal = 0;
              bw.write(symbol + " " + packetFromClient.quote + "\n");
            }
          }
          else
          {
            System.out.println("ERROR: OnlineBrokerHandlerThread: quote_get: file format is wrong!");
            break;
          }
        }
        else
        {
          System.out.println("ERROR: OnlineBrokerHandlerThread: quote_get: file format is wrong!");
          break;
        }
      }
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
    try
    {
      if (br != null && bw != null)
      {
        br.close();
        bw.close();
      }

      File origFile = new File(exchange);
      File tempFile = new File(exchange + "Temp");

      if (returnVal == ERROR_HANDLE_EXCHANGE_QUOTE_DNE)
      {
        if (! tempFile.delete() )
        {
          System.out.println("ERROR: symbol_update: Could not delete temp file!");
        }
        return returnVal;
      }

      if(! origFile.delete() )
      {
        System.out.println("ERROR: symbol_update: Could not remove input file!");
      }
      if(! tempFile.renameTo(origFile) )
      {
        System.out.println("ERROR: symbol_update: Could not rename output file!");
      }

      return returnVal;
    }
    catch (IOException ex)
    {
      ex.printStackTrace();
    }
    return returnVal;
  }

  // TSE & NASDAQ are hardwired so this logic is required to find the other exchange.
  private String otherExchangeName()
  {
    if ( exchange.equalsIgnoreCase("nasdaq") )
    {
      return "tse";
    }
    return "nasdaq";
  }

}




 
