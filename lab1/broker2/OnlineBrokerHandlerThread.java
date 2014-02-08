import java.net.*;
import java.io.*;

public class OnlineBrokerHandlerThread extends Thread {
  private Socket socket = null;
  private BrokerPacket packetFromClient = null;
  private BrokerPacket packetToClient = null;

  private static final int ERROR_HANDLE_EXCHANGE_QUOTE_DNE = 1;

  private static final int QUOTE_LOWERBOUND = 1;
  private static final int QUOTE_UPPERBOUND = 300;

  public OnlineBrokerHandlerThread(Socket socket)
  {
    super("OnlineBrokerHandlerThread");
    this.socket = socket;
    System.out.println("Created new Thread to handle client(" + socket.getInetAddress() + ")");
  }

  public void run()
  {

    boolean gotByePacket = false;
    
    try
    {
      /* stream to read from client */
      ObjectInputStream fromClient = new ObjectInputStream(socket.getInputStream());

      /* stream to write back to client */
      ObjectOutputStream toClient = new ObjectOutputStream(socket.getOutputStream());

      while (( packetFromClient = (BrokerPacket) fromClient.readObject()) != null)
      {
        if (packetFromClient.type > BrokerPacket.BROKER_NULL && packetFromClient.type < BrokerPacket.BROKER_BYE)
        {
          handleClient();

          // send reply back to client 
          toClient.writeObject(packetToClient);
        }

        // EXCHANGE_ADD = 201  and  EXCHANGE_REPLY = 205
        else if (packetFromClient.type > 200 && packetFromClient.type < 205)
        {
          handleExchange();
          toClient.writeObject(packetToClient);
        }
        
        // Sending an BROKER_NULL || BROKER_BYE means quit
        else if (packetFromClient.type == BrokerPacket.BROKER_NULL || packetFromClient.type == BrokerPacket.BROKER_BYE)
        {
          gotByePacket = true;
          System.out.println("A client(" + socket.getInetAddress() + ") has disconnected.");
          break;
        }
        
        // If code comes here, there is an error in the packet
        else
        {
          System.err.println("ERROR: Unknown ECHO_* packet!!");
          System.exit(-1);
        }
      }
      
      /* cleanup when client exits */
      fromClient.close();
      toClient.close();
      socket.close();

    }
    catch (IOException e)
    {
      if(! gotByePacket)
        e.printStackTrace();
    }
    catch (ClassNotFoundException e)
    {
      if(! gotByePacket)
        e.printStackTrace();
    }
  }

  private void handleClient()
  {  
    // Process message
    if(packetFromClient.type == BrokerPacket.BROKER_REQUEST && packetFromClient.symbol != null)
    {
      packetToClient = new BrokerPacket();
      System.out.println("From a client: " + packetFromClient.symbol);

      Long quoteVal = quote_get();

      if (quoteVal != -1)
      {
        packetToClient.type = BrokerPacket.BROKER_QUOTE;
        packetToClient.quote = quoteVal;
        return;
      }
      packetToClient.type = BrokerPacket.ERROR_INVALID_SYMBOL;
      return;
    }
    else
    {
      System.out.println("ERROR: handleClient: wrong packetFromClient type or NULL symbol!");
      System.out.println("type: " + packetFromClient.type);
    }

    return;
  }


  private void handleExchange()
  {
    if (packetFromClient.type == BrokerPacket.EXCHANGE_ADD && packetFromClient.symbol != null)
    {
      packetToClient = new BrokerPacket();
      System.out.println("From a exchange: add " + packetFromClient.symbol);

      // Lookup for existing quote
      Long quoteVal = quote_get();

      // If symbol match is found
      if (quoteVal != -1)
      {
        packetToClient.type = BrokerPacket.ERROR_SYMBOL_EXISTS;
        return;
      }

      symbol_add();
    }
    else if (packetFromClient.type == BrokerPacket.EXCHANGE_REMOVE && packetFromClient.symbol != null)
    {
      packetToClient = new BrokerPacket();
      System.out.println("From a exchange: remove " + packetFromClient.symbol);

      if ( symbol_remove() != 0 )
      {
        packetToClient.type = BrokerPacket.ERROR_INVALID_SYMBOL;
        return;
      }
    }
    else if (packetFromClient.type == BrokerPacket.EXCHANGE_UPDATE && packetFromClient.symbol != null)
    {
      packetToClient = new BrokerPacket();
      Long quoteVal = packetFromClient.quote;
      
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
    }
    else
    {
      System.out.println("ERROR: handleExchange: wrong packetFromClient type or NULL symbol!");
      System.out.println("type: " + packetFromClient.type);
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
      br = new BufferedReader( new FileReader("nasdaq") );
 
      while ( (sCurrentLine = br.readLine() ) != null )
      {
        if ( sCurrentLine.contains(" ") )
        {
          // Tokenize the line with space delimeter and check for a match
          String[] tokens = sCurrentLine.split(delims);
          if (tokens.length == 2)
          {
            if ( tokens[0].equalsIgnoreCase(packetFromClient.symbol) )
            {
              quoteVal = Long.parseLong(tokens[1]);
              break;
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
      BufferedWriter bw = new BufferedWriter( new FileWriter("nasdaq", true) );

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
      br = new BufferedReader( new FileReader("nasdaq") );
      bw = new BufferedWriter( new FileWriter("nasdaqTemp") );
 
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
      File origFile = new File("nasdaq");
      File tempFile = new File("nasdaqTemp");
 
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
      br = new BufferedReader( new FileReader("nasdaq") );
      bw = new BufferedWriter( new FileWriter("nasdaqTemp") );
 
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

      File origFile = new File("nasdaq");
      File tempFile = new File("nasdaqTemp");

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

}




 
