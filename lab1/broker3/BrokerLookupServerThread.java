import java.net.*;
import java.io.*;
// import java.util.LinkedList;

import java.util.ArrayList;
import java.util.Arrays;

import java.util.concurrent.ConcurrentHashMap;

public class BrokerLookupServerThread extends Thread
{
  private Socket socket = null;
  private static ConcurrentHashMap<String, ArrayList<BrokerLocation>> brokerHash = null;

  public BrokerLookupServerThread(Socket socket, ConcurrentHashMap hm)
  {
    super("BrokerLookupServerThread");
    this.socket = socket;
    this.brokerHash = hm;
    try
    {
      System.out.println("Created new Thread to handle client(" + InetAddress.getLocalHost().getHostAddress() + ")");
    }
    catch (UnknownHostException uhe)
    {
      System.err.println("ERROR: Unknown host!");
    }
  }

  public void run()
  {
    boolean gotByePacket = false;
    
    try
    {
      // Stream to read/write from client
      ObjectInputStream fromClient = new ObjectInputStream( socket.getInputStream() );
      ObjectOutputStream toClient = new ObjectOutputStream( socket.getOutputStream() );
      BrokerPacket packetFromClient = null;
      BrokerPacket packetToClient = null;
      ArrayList<BrokerLocation> al_BrokerLocation = null;

      while ( ( packetFromClient = (BrokerPacket) fromClient.readObject()) != null )
      {
        packetToClient = new BrokerPacket();
        packetToClient.exchange = packetFromClient.exchange;
        
        switch(packetFromClient.type)
        {
          case BrokerPacket.LOOKUP_REQUEST:
            System.out.print("LOOKUP_REQUEST: " + packetFromClient.exchange);
            if( (al_BrokerLocation = brokerHash.get(packetFromClient.exchange) ) != null)
            {
                System.out.println(" Found.");
                packetToClient.type = BrokerPacket.LOOKUP_REPLY;
                packetToClient.num_locations = al_BrokerLocation.size();
                packetToClient.locations = al_BrokerLocation.toArray(new BrokerLocation[0]);
            }
            else
            {
                System.out.println(" not found!");
                packetToClient.type = BrokerPacket.ERROR_INVALID_EXCHANGE;
                packetToClient.num_locations = 0;
            }
            // Send reply back to client 
            toClient.writeObject(packetToClient);
            break;

          case BrokerPacket.LOOKUP_REGISTER:
            System.out.print("LOOKUP_REGISTER: ");
            al_BrokerLocation = brokerHash.get(packetFromClient.exchange);
            if(al_BrokerLocation != null)
            {
              if (! al_BrokerLocation.addAll( Arrays.asList(packetFromClient.locations) ) )
              {
                System.out.println("Inserting into brokerHash failed");
              }
            } 
            else
            {
              brokerHash.put( packetFromClient.exchange, new ArrayList<BrokerLocation>( Arrays.asList(packetFromClient.locations) ) );
            }

            // Send ACK with just received data
            packetToClient.type = BrokerPacket.LOOKUP_REPLY;
            packetToClient.num_locations = 1;
            packetToClient.locations = packetFromClient.locations;

            System.out.println(packetFromClient.exchange + " registration successful.");
            toClient.writeObject(packetToClient);
            break;

          // Sending an BROKER_NULL || BROKER_BYE means quit
          case BrokerPacket.BROKER_NULL:
          case BrokerPacket.BROKER_BYE:
            gotByePacket = true;
            System.out.println("A client(" + InetAddress.getLocalHost().getHostAddress() + ") has disconnected.");
            break;

          // If code comes here, there is an error in the packet
          default:
            System.err.println("ERROR: Invalid LOOKUP_* packet!!");
            System.exit(-1);
            break;
        }
      }
      
      // cleanup when client exits
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

}




 
