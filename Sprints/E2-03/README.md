\# Sprint E2-03 — Comunicação Líder–Réplicas, Colocação e Escrita Replicada



\## 1. Formato das Mensagens e Tópicos

Mensagens em texto UTF-8 separadas por `;` com binários em Base64.



\* \*\*Tópicos:\*\*

&#x20; \* `tpf-r-<id>`: líder → réplica `<id>` (`STORE`).

&#x20; \* `tpf-ctl`: líder → todas as réplicas (difusão de controlo).

&#x20; \* `tpf-lider`: réplicas → líder em processo (`HB`, `INVENTARIO`, `ACK`).

\* \*\*1.º Registo:\*\* `OLA;<id>;<chavePublicaB64>;<token>`

\* \*\*Ligações seguintes:\*\* `OLA;<id>`

\* \*\*Desafio / Prova:\*\* `DESAFIO;<nonceB64>` (32 bytes `SecureRandom`) → `PROVA;<id>;<assinaturaB64>` (Ed25519 sobre `"tpf-registo|" + id + "|" + nonceB64`).

\* \*\*Subscrição / Publicação:\*\* `SUB;<topico>` e `PUB;<topico>;<mensagem>`

\* \*\*Heartbeat e Inventário:\*\* `HB;<id>;<usados>;<quota>` (a cada `2000 ms`) e `INVENTARIO;<id>;<chunkId1>,<chunkId2>,...`

\* \*\*Escrita Replicada:\*\* `STORE;<chunkId>;<hdrB64>;<binB64>` → `ACK;<id>;<chunkId>;<sha256>`



\## 2. Como Lançar o Líder e N Réplicas

1\. Colocar `token.txt`, `guild.txt` e `master.key` (32 bytes) em `tpf-lider/` e arrancar `tpf.LeaderBot`.

2\. Gerar um token por réplica no Discord com `/replica-token` (papel `admin-arquivo`, validade 10 min).

3\. Compilar e lançar N réplicas em `tpf-replica/`:

&#x20;  ```bash

&#x20;  javac -encoding UTF-8 -d target/classes src/main/java/tpf/\*.java

&#x20;  java -cp target/classes tpf.ReplicaService A ws://localhost:8765 8388608 <TOKEN\_A>

&#x20;  java -cp target/classes tpf.ReplicaService B ws://localhost:8765 2097152 <TOKEN\_B>

&#x20;  java -cp target/classes tpf.ReplicaService C ws://localhost:8765 4194304 <TOKEN\_C>

