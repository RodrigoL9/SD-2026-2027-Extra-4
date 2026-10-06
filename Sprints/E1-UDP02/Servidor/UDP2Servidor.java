import java.net.*;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

public class UDP2Servidor {

    // CA2: As duas estruturas de dados
    // Lista sequencial para mensagens entregues em ordem (operação frequente: adicionar ao fim)
    static ArrayList<String> listaRececao = new ArrayList<>();

    // Hash para mensagens adiantadas (operação frequente: procurar se a chave N existe)
    static HashMap<Integer, String> estruturaTemporaria = new HashMap<>();

    // Lista auxiliar apenas para imprimir no ecrã o que foi entregue em cada passo
    static ArrayList<String> entreguesNestePasso = new ArrayList<>();

    /**
     * Processes delivered messages
     * @return the last message processed in order
     */
    public static int processDeliveredMessages(int nLastMessageInOrder, int nCurrentMessage, String currentMessage) {
        entreguesNestePasso.clear();

        // Tratamento de duplicados de mensagens já entregues (N <= L)
        if (nCurrentMessage <= nLastMessageInOrder) {
            return nLastMessageInOrder; // Ignora e não altera o L
        }

        // Cenário 1: Mensagem chega em ordem
        if (nCurrentMessage == nLastMessageInOrder + 1) {
            listaRececao.add(currentMessage);
            entreguesNestePasso.add(nCurrentMessage + ", " + currentMessage);
            int novoL = nCurrentMessage;

            // CA3: A Entrega em Cascata
            // Verifica se a próxima mensagem já está à espera na estrutura temporária
            while (estruturaTemporaria.containsKey(novoL + 1)) {
                novoL++;
                String msgGuardada = estruturaTemporaria.get(novoL);

                // Entrega a mensagem guardada e remove-a da hash
                listaRececao.add(msgGuardada);
                entreguesNestePasso.add(novoL + ", " + msgGuardada);
                estruturaTemporaria.remove(novoL);
            }
            return novoL; // Devolve o novo estado de L
        }
        // Cenário 2: Mensagem chega adiantada (fora de ordem)
        else {
            // O HashMap substitui chaves iguais, logo também trata duplicados não entregues
            estruturaTemporaria.put(nCurrentMessage, currentMessage);
            return nLastMessageInOrder; // O L não avança
        }
    }

    public static void main(String args[]) {
        DatagramSocket aSocket = null;
        int L = 0; // O estado: número da última mensagem recebida EM ORDEM

        try {
            aSocket = new DatagramSocket(6789);
            byte[] buffer = new byte[1000];
            System.out.println("Servidor UDP02 iniciado. À espera de mensagens...");

            while (true) {
                DatagramPacket request = new DatagramPacket(buffer, buffer.length);
                aSocket.receive(request);

                String recebido = new String(request.getData(), 0, request.getLength()).trim();
                String respostaTexto = "";

                try {
                    String[] partes = recebido.split(",", 2);
                    if (partes.length < 2) throw new NumberFormatException("Sem vírgula");

                    int N = Integer.parseInt(partes[0].trim());
                    String textoMensagem = partes[1].trim();

                    int lAnterior = L;

                    // Chama o método com contrato bem definido
                    L = processDeliveredMessages(L, N, textoMensagem);

                    // Regra de decisão para a resposta
                    if (L == lAnterior) {
                        // Se o L não avançou, a mensagem foi para a Hash (ou é repetida)
                        respostaTexto = "waitingfor," + (L + 1);
                    } else {
                        // Se o L avançou, a mensagem foi entregue
                        respostaTexto = recebido;
                    }

                    // CA4: Prints obrigatórios para demonstração
                    System.out.println("\n-> Recebido: " + recebido);
                    System.out.println("   L atual: " + L);
                    System.out.println("   Estrutura temporária: " + estruturaTemporaria);
                    System.out.println("   Entregues neste passo: " + entreguesNestePasso);

                } catch (Exception e) {
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