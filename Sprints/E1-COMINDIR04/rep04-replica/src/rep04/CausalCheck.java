package rep04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class CausalCheck {

    public static void main(String[] args) {

        if (args.length < 1) {
            System.out.println(
                    "Uso: CausalCheck <ficheiro1> [ficheiro2 ...]"
            );
            return;
        }

        boolean tudoOk = true;

        for (String nomeFicheiro : args) {

            System.out.println();
            System.out.println(
                    "=== Verificar: " + nomeFicheiro + " ==="
            );

            boolean ok = verificar(nomeFicheiro);

            if (!ok) {
                tudoOk = false;
            }
        }

        System.out.println();

        if (tudoOk) {
            System.out.println(
                    "CAUSAL CHECK: OK"
            );
        } else {
            System.out.println(
                    "CAUSAL CHECK: FALHOU"
            );
        }
    }


    private static boolean verificar(String nomeFicheiro) {

        Path path = Path.of(nomeFicheiro);

        if (!Files.exists(path)) {
            System.out.println(
                    "ERRO: ficheiro não encontrado."
            );
            return false;
        }

        try {

            List<String> linhas =
                    Files.readAllLines(
                            path,
                            StandardCharsets.UTF_8
                    );

            SensorRecord anterior = null;

            long esperado = 1;

            int total = 0;

            boolean ok = true;


            for (String linha : linhas) {

                if (linha.isBlank()) {
                    continue;
                }

                SensorRecord atual;

                try {

                    atual =
                            SensorRecord.fromLine(
                                    linha
                            );

                } catch (Exception e) {

                    System.out.println(
                            "ERRO: registo mal formado:"
                    );

                    System.out.println(
                            linha
                    );

                    ok = false;
                    continue;
                }


                total++;


                /*
                 * 1. Verificar sequência.
                 */
                if (atual.getSeq() != esperado) {

                    System.out.println(
                            "ERRO SEQ: esperado " +
                                    esperado +
                                    " mas encontrado " +
                                    atual.getSeq()
                    );

                    ok = false;
                }

                esperado =
                        atual.getSeq() + 1;


                /*
                 * 2. Na partição do líder,
                 * o componente correspondente do
                 * vetor deve ser igual ao seq.
                 *
                 * A partição é obtida através
                 * do nome do ficheiro.
                 */
                Integer particao =
                        extrairParticao(
                                nomeFicheiro
                        );

                if (particao != null) {

                    try {

                        long valorVector =
                                atual
                                        .getVector()
                                        .get(particao);

                        if (valorVector
                                != atual.getSeq()) {

                            System.out.println(
                                    "ERRO VETOR: seq=" +
                                            atual.getSeq() +
                                            " mas V[" +
                                            particao +
                                            "]=" +
                                            valorVector
                            );

                            ok = false;
                        }

                    } catch (Exception e) {

                        System.out.println(
                                "ERRO VETOR em seq=" +
                                        atual.getSeq()
                        );

                        ok = false;
                    }
                }


                /*
                 * 3. Lamport deve crescer.
                 */
                if (anterior != null) {

                    if (atual.getLamport()
                            <= anterior.getLamport()) {

                        System.out.println(
                                "ERRO LAMPORT: seq " +
                                        anterior.getSeq() +
                                        " tem L=" +
                                        anterior.getLamport() +
                                        " e seq " +
                                        atual.getSeq() +
                                        " tem L=" +
                                        atual.getLamport()
                        );

                        ok = false;
                    }


                    /*
                     * 4. Dentro da mesma partição,
                     * os eventos consecutivos devem
                     * respeitar a ordem causal.
                     *
                     * anterior < atual
                     */
                    VectorClock.Order ordem =
                            anterior
                                    .getVector()
                                    .compare(
                                            atual.getVector()
                                    );

                    if (ordem
                            != VectorClock.Order.BEFORE) {

                        System.out.println(
                                "ERRO CAUSAL: seq " +
                                        anterior.getSeq() +
                                        " -> seq " +
                                        atual.getSeq() +
                                        " | ordem=" +
                                        ordem
                        );

                        ok = false;
                    }
                }


                anterior = atual;
            }


            if (total == 0) {

                System.out.println(
                        "AVISO: ficheiro vazio."
                );

                return false;
            }


            if (ok) {

                System.out.println(
                        "OK: " +
                                total +
                                " registos verificados."
                );
            }


            return ok;


        } catch (IOException e) {

            System.out.println(
                    "ERRO ao ler ficheiro: " +
                            e.getMessage()
            );

            return false;
        }
    }


    /*
     * Exemplos aceites:
     *
     * replica-1-p0.txt -> 0
     * replica-2-p1.txt -> 1
     * lider-p2.txt     -> 2
     */
    private static Integer extrairParticao(
            String nomeFicheiro) {

        String nome =
                Path.of(nomeFicheiro)
                        .getFileName()
                        .toString();

        int pos =
                nome.lastIndexOf("-p");

        if (pos < 0) {
            return null;
        }

        int inicio =
                pos + 2;

        int fim =
                nome.indexOf(
                        ".",
                        inicio
                );

        if (fim < 0) {
            fim = nome.length();
        }

        try {

            return Integer.parseInt(
                    nome.substring(
                            inicio,
                            fim
                    )
            );

        } catch (NumberFormatException e) {

            return null;
        }
    }
}
