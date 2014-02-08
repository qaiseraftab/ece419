import java.net.*;

public class exchange {
	public static void main(String[] args) throws IOException,
			ClassNotFoundException {

		Socket BrokerSocket = null;
		ObjectOutputStream out = null;
		ObjectInputStream in = null;

		try {
			/* variables for hostname/port */
			String hostname = "localhost";
			int port = 4444;
			
			if(args.length == 2 ) {
				hostname = args[0];
				port = Integer.parseInt(args[1]);
			} else {
				System.err.println("ERROR: Invalid arguments!");
				System.exit(-1);
			}
			BrokerSocket = new Socket(hostname, port);

			out = new ObjectOutputStream(BrokerSocket.getOutputStream());
			in = new ObjectInputStream(BrokerSocket.getInputStream());

		} catch (UnknownHostException e) {
			System.err.println("ERROR: Don't know where to connect!!");
			System.exit(1);
		} catch (IOException e) {
			System.err.println("ERROR: Couldn't get I/O for the connection.");
			System.exit(1);
		}

		BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));
		String userInput;

		System.out.print("CONSOLE>");
		while ((userInput = stdIn.readLine()) != null
				&& userInput.toLowerCase().indexOf("x") == -1) {
			/* make a new request packet */
			BrokerPacket packetToServer = new BrokerPacket();
			/*figure out the type of packet we want to send (add,remove or update)*/ 				if( userInput.contains("add"))
				packetToServer.type = BrokerPacket.EXCHANGE_ADD;
			else if (userInput.contains("remove"))
				packetToServer.type = BrokerPacket.EXCHANGE_REMOVE;
			else if (userInput.contains("update"))
				packetToServer.type = BrokerPacket.EXCHANGE_UPDATE;	
			else
				System.out.println("Invalid input. Must enter add,remove or update");	


			// userinput 			
			packetToServer.symbol =userInput;
			out.writeObject(packetToServer);

			/* print server reply */
			BrokerPacket packetFromServer;
			packetFromServer = (BrokerPacket) in.readObject();

			if (packetFromServer.type == BrokerPacket.BROKER_QUOTE)
				System.out.println("Quote from broker: " + packetFromServer.quote);

			/* re-print console prompt */
			System.out.print("CONSOLE>");
		}

		/* tell server that i'm quitting */
		BrokerPacket packetToServer = new BrokerPacket();
		packetToServer.type = BrokerPacket.BROKER_BYE;
		//send 1000 for bye		
		packetToServer.symbol = "x";
		//packetToServer.symbol = "x";
		out.writeObject(packetToServer);

		out.close();
		in.close();
		stdIn.close();
		BrokerSocket.close();
	}
}
