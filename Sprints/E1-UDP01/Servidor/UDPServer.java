import java.net.*;
import java.io.*;

public class UDPServer {
    public static void main(String args[]) {
        DatagramSocket aSocket = null;
        int L = 0; // ESTADO: número da última mensagem recebida EM ORDEM. Inicia a 0.

        try {
            aSocket = new DatagramSocket(6789);
            byte[] buffer = new byte[1000];
            System.out.println("Servidor UDP iniciado no porto 6789. À espera de mensagens...");

            while (true) {
                DatagramPacket request = new DatagramPacket(buffer, buffer.length);
                aSocket.receive(request);

                // Transformamos os bytes recebidos numa String limpa
                String recebido = new String(request.getData(), 0, request.getLength()).trim();
                String respostaTexto = "";

                try {
                    // Vamos separar o número da mensagem pelo caracter vírgula
                    String[] partes = recebido.split(",", 2);
                    if (partes.length < 2) {
                        throw new NumberFormatException("Sem vírgula");
                    }

                    int N = Integer.parseInt(partes[0].trim());

                    // REGRA DE DECISÃO
                    if (N == L + 1) {
                        L = N; // Atualiza o estado
                        respostaTexto = recebido; // Comportamento de echo normal
                    } else {
                        // Mensagem fora de ordem (N diferente de L+1)
                        respostaTexto = "waitingfor," + (L + 1);
                    }
                } catch (Exception e) {
                    // Se a mensagem vier mal formada (ex: sem número), pedimos a mensagem esperada na mesma.
                    // O servidor NUNCA vai abaixo (cumpre o requisito do CA3).
                    respostaTexto = "waitingfor," + (L + 1);
                }

                byte[] respostaBytes = respostaTexto.getBytes();
                DatagramPacket reply = new DatagramPacket(
                        respostaBytes, respostaBytes.length,
                        request.getAddress(), request.getPort()
                );

                aSocket.send(reply);
            }
        } catch (SocketException e) { System.out.println("Socket: " + e.getMessage());
        } catch (IOException e)     { System.out.println("IO: " + e.getMessage());
        } finally { if (aSocket != null) aSocket.close(); }
    }
}