import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class NamingService
{
  private Socket socket = null;
  private ObjectOutputStream toServer = null;
  private ObjectInputStream fromServer = null;

  public NamingService(String nameServer_hostName, int nameServer_port)
         throws IOException, ClassNotFoundException
  {
    this.socket = new Socket(nameServer_hostName, nameServer_port);
    this.toServer = new ObjectOutputStream( socket.getOutputStream() );
    this.fromServer = new ObjectInputStream( socket.getInputStream() );
  }

  public boolean registerToNameServer(String exchange, String host, int port) 
         throws IOException, ClassNotFoundException
  {
    boolean isSuccessful = false;

    // Register the exchange to the name server
    BrokerPacket packetToServer = new BrokerPacket();
    packetToServer.type = BrokerPacket.LOOKUP_REGISTER;
    packetToServer.exchange = exchange;
    packetToServer.num_locations = 1;
    packetToServer.locations = new BrokerLocation[] { new BrokerLocation(host, port) };

    toServer.writeObject(packetToServer);

    // Check valeus for acknowledgement
    BrokerPacket packetFromServer = (BrokerPacket) fromServer.readObject();
    if (packetFromServer != null && 
        packetFromServer.type == BrokerPacket.LOOKUP_REPLY && 
        exchange.equals(packetFromServer.exchange) &&
        packetFromServer.locations != null &&
        packetFromServer.locations[0].broker_host.equals(host) &&
        packetFromServer.locations[0].broker_port == port
       )
    {
          isSuccessful = true;
    }
    else
    {
        System.out.println("ERROR: NamingService: registerToNameServer: packetFromServer has wrong info!");
        System.exit(1);
    }

    return isSuccessful;
  }

  public BrokerLocation lookupNameServer(String exchange) 
         throws IOException, ClassNotFoundException
  {
    BrokerPacket packetToServer = new BrokerPacket();
    packetToServer.type = BrokerPacket.LOOKUP_REQUEST;
    packetToServer.exchange = exchange;

    toServer.writeObject(packetToServer);

    BrokerPacket packetFromServer = (BrokerPacket) fromServer.readObject();
    BrokerLocation location = null;
    if (packetFromServer != null && 
        packetFromServer.type == BrokerPacket.LOOKUP_REPLY &&
        exchange.equals(packetFromServer.exchange) && 
        packetFromServer.num_locations > 0 &&
        packetFromServer.locations.length > 0
       ) 
    {
      
      // Assume the name server returns the best fit on the first element so choose the first one.
      location = packetFromServer.locations[0];
    }
    // If above statement is false, the packet type must be "ERROR_INVALID_EXCHANGE" or it is failure.
    else if (packetFromServer.type != BrokerPacket.ERROR_INVALID_EXCHANGE)
    {
        System.out.println("ERROR: NamingService: lookupNameServer: packetFromServer has wrong info!");
        System.exit(1);
    }    

    return location;
  }

  public void closeNameServerConnection() throws IOException
  {
    // Send bye packet to notify the name server
    BrokerPacket packetToServer = new BrokerPacket();
    packetToServer.type = BrokerPacket.BROKER_BYE;

    toServer.writeObject(packetToServer);

    toServer.close();
    fromServer.close();
    socket.close();
  }
}
