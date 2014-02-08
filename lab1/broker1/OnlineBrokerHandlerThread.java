import java.net.*;
import java.io.*;

public class OnlineBrokerHandlerThread extends Thread {
  private Socket socket = null;
  private BrokerPacket packetFromClient = null;
  private BrokerPacket packetToClient = null;

  public OnlineBrokerHandlerThread(Socket socket)
  {
    super("OnlineBrokerHandlerThread");
    this.socket = socket;
    System.out.println("Created new Thread (" + Thread.currentThread().getId() + ")to handle client(" + socket.getInetAddress() + ")");
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
        /* create a packet to send reply back to client */
        packetToClient = new BrokerPacket();
        packetToClient.type = BrokerPacket.BROKER_QUOTE;
      
        // Process message
        if(packetFromClient.type == BrokerPacket.BROKER_REQUEST && packetFromClient.symbol != null)
        {
          System.out.println("Symbol got from a client: " + packetFromClient.symbol);
          lookupQuote();
        
          // send reply back to client 
          toClient.writeObject(packetToClient);
          
          // wait for next packet 
          continue;
        }
        
        // Sending an BROKER_NULL || BROKER_BYE means quit
        if (packetFromClient.type == BrokerPacket.BROKER_NULL || packetFromClient.type == BrokerPacket.BROKER_BYE) {
          gotByePacket = true;
          System.out.println("A client(" + socket.getInetAddress() + ") has disconnected.");
          break;
        }
        
        /* if code comes here, there is an error in the packet */
        System.err.println("ERROR: Unknown ECHO_* packet!!");
        System.exit(-1);
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

  private void lookupQuote()
  {
    BufferedReader br = null;
    try
    {
      String sCurrentLine;
      String delims = "[ ]";
      br = new BufferedReader( new FileReader("nasdaq") );
      packetToClient.quote = 0L;
 
      while ( (sCurrentLine = br.readLine() ) != null )
      {
        if ( sCurrentLine.contains(" ") )
        {
          String[] tokens = sCurrentLine.split(delims);
          if ( tokens[0].equalsIgnoreCase(packetFromClient.symbol) )
          {
            packetToClient.quote = Long.parseLong(tokens[1]);
          }
        }
      }
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
    finally
    {
      try
      {
        if (br != null)
          br.close();
      }
      catch (IOException ex)
      {
        ex.printStackTrace();
      }
    }
  }

}




 
