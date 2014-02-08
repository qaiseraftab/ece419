import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.*;

public class ConnectionWrapper
{
  private Socket socket = null;
  private ObjectOutputStream outStream = null;
  private ObjectInputStream inStream = null;

  public ConnectionWrapper(String nameServer_hostName, int nameServer_port)
         throws IOException, ClassNotFoundException, UnknownHostException
  {
    this.socket = new Socket(nameServer_hostName, nameServer_port);
    this.outStream = new ObjectOutputStream( socket.getOutputStream() );
    this.inStream = new ObjectInputStream( socket.getInputStream() );
  }

  public ConnectionWrapper(Socket skt)
         throws IOException, ClassNotFoundException
  {
    this.socket = skt;
    this.outStream = new ObjectOutputStream( skt.getOutputStream() );
    this.inStream = new ObjectInputStream( skt.getInputStream() );
  }

  public void sendPacket(BrokerPacket bp) throws IOException
  {
    outStream.writeObject(bp);
  }

  public BrokerPacket getPacket() throws IOException
  {
    BrokerPacket bp = null;
    try
    {
      bp = (BrokerPacket) inStream.readObject();
    }
    catch (ClassNotFoundException e)
    {
      System.err.println("ERROR: ConnectionWrapper: getPacket: readObject failed!");
      e.printStackTrace();
      System.exit(-1);
    }
    return bp;
  }

  public void closeConnection_serverSide() throws IOException
  {
    // Server doesn't need to send bye packet since
    // the client already closed the connection.
    outStream.close();
    inStream.close();
    socket.close();
  }

  public void closeConnection_clientSide() throws IOException
  {
    // Send bye packet to notify termination
    BrokerPacket packetToServer = new BrokerPacket();
    packetToServer.type = BrokerPacket.BROKER_BYE;
    packetToServer.symbol = "Bye!";
    outStream.writeObject(packetToServer);

    outStream.close();
    inStream.close();
    socket.close();
  }
}
