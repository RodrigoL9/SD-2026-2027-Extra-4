import java.net.*;
import java.io.*;
import java.util.Scanner;

public class UDP2Cliente {
    public static void main(String args[]) {
        DatagramSocket aSocket = null;
        try {
            aSocket = new DatagramSocket();
            InetAddress aHost = InetAddress.getByName("localhost");
            int serverPort = 6789;
            Scanner scanner = new Scanner(System.in);

            System.out.println("=== Cliente UDP ===");
            System.out.println("Modos de numeração:");
            System.out.println("1 - Automático (O cliente numera as mensagens)");
            System.out.println("2 - Manual (Tu inseres o número da mensagem)");
            System.out.print("Escolhe uma opção (1 ou 2): ");
            String opcao = scanner.nextLine();

            boolean modoAuto = opcao.equals("1");
            int numSeqAuto = 1;

            System.out.println("\n(Escreve 'sair' a qualquer momento para fechar o cliente)");

            while (true) {
                String mensagemFinal = "";

                if (modoAuto) {
                    System.out.print("Texto da mensagem: ");
                    String texto = scanner.nextLine();
                    if (texto.equalsIgnoreCase("sair")) break;

                    mensagemFinal = numSeqAuto + "," + texto;
                    numSeqAuto++;
                } else {
                    System.out.print("Número de sequência (N): ");
                    String numStr = scanner.nextLine();
                    if (numStr.equalsIgnoreCase("sair")) break;

                    System.out.print("Texto da mensagem: ");
                    String texto = scanner.nextLine();
                    if (texto.equalsIgnoreCase("sair")) break;

                    mensagemFinal = numStr + "," + texto;
                }

                byte[] m = mensagemFinal.getBytes();
                DatagramPacket request = new DatagramPacket(m, m.length, aHost, serverPort);
                aSocket.send(request);

                byte[] buffer = new byte[1000];
                DatagramPacket reply = new DatagramPacket(buffer, buffer.length);
                aSocket.receive(reply);

                // Lemos a resposta respeitando o getLength() (requisito do CA2)
                String resposta = new String(reply.getData(), 0, reply.getLength());

                // Diferenciar graficamente o erro do echo normal
                if (resposta.startsWith("waitingfor,")) {
                    System.out.println("⚠️ ERRO - Servidor diz: " + resposta + "\n");
                } else {
                    System.out.println("✅ ECHO - Recebido: " + resposta + "\n");
                }
            }
            scanner.close();

        } catch (SocketException e) { System.out.println("Socket: " + e.getMessage());
        } catch (IOException e)     { System.out.println("IO: " + e.getMessage());
        } finally { if (aSocket != null) aSocket.close(); }
    }
}