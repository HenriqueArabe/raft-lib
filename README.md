# raft-lib

Biblioteca modular do algoritmo de consenso [Raft](https://raft.github.io/) em Go puro, sem dependências externas.

Desenvolvida como parte do Trabalho de Conclusão de Curso II na Universidade Presbiteriana Mackenzie.

## Funcionalidades

- **Eleição de líder** com timeouts aleatorizados
- **Replicação de log** com consistência garantida
- **Snapshots** para compactação de log
- **Mudança de configuração** (adição/remoção de nós em runtime)
- **Persistência de estado** para recuperação após falhas
- **Deduplicação de comandos** por cliente (ClientID + SeqNum)
- **Métricas** de latência, eleições e commits
- **Zero dependências externas** — usa apenas a biblioteca padrão do Go

## Arquitetura

A biblioteca é organizada em quatro pacotes com responsabilidades bem definidas:

```
raft-lib/
├── types/       # Tipos compartilhados, RPCs e interface StateMachine
├── transport/   # Interface Transport + implementação net/rpc (RPCTransport)
├── storage/     # Interface Storage + implementação em memória (MemoryStorage)
└── raft/        # Lógica Raft (eleição, replicação, snapshots, config change)
```

O pacote `raft` importa `types`, `transport` e `storage`. Não há importações circulares.

## Instalação

```bash
go get github.com/henrique-arab/raft-lib
```

Requer Go 1.21 ou superior.

## Guia Rápido

### 1. Implementar a StateMachine

A aplicação deve implementar a interface `types.StateMachine`:

```go
type StateMachine interface {
    Apply(command []byte) error
    Snapshot() ([]byte, error)
    Restore(snapshot []byte) error
}
```

Exemplo com um mapa chave-valor:

```go
type KVSM struct {
    mu   sync.RWMutex
    data map[string]string
}

func (kv *KVSM) Apply(command []byte) error {
    var op Op
    if err := json.Unmarshal(command, &op); err != nil {
        return err
    }
    kv.mu.Lock()
    defer kv.mu.Unlock()
    switch op.Kind {
    case "set":
        kv.data[op.Key] = op.Value
    case "del":
        delete(kv.data, op.Key)
    }
    return nil
}

func (kv *KVSM) Snapshot() ([]byte, error) {
    kv.mu.RLock()
    defer kv.mu.RUnlock()
    return json.Marshal(kv.data)
}

func (kv *KVSM) Restore(snapshot []byte) error {
    kv.mu.Lock()
    defer kv.mu.Unlock()
    kv.data = make(map[string]string)
    return json.Unmarshal(snapshot, &kv.data)
}
```

### 2. Criar e iniciar o nó

```go
import (
    "github.com/HenriqueArabe/raft-lib/raft"
    "github.com/HenriqueArabe/raft-lib/storage"
    "github.com/HenriqueArabe/raft-lib/transport"
    "github.com/HenriqueArabe/raft-lib/types"
)

cfg := types.Config{
    ID:            "127.0.0.1:9001",
    Peers:         []types.ServerID{"127.0.0.1:9002", "127.0.0.1:9003"},
    HeartbeatMs:   50,
    ElectionMinMs: 300,
    ElectionMaxMs: 500,
}

sm := NewKVSM()
tr := transport.NewRPCTransport(cfg.ID)
st := storage.NewMemoryStorage()

node, err := raft.New(cfg, sm, tr, st)
if err != nil {
    log.Fatal(err)
}
if err := node.Start(); err != nil {
    log.Fatal(err)
}
defer node.Stop()
```

### 3. Submeter comandos

```go
data, _ := json.Marshal(Op{Kind: "set", Key: "x", Value: "42"})

resp, err := node.Apply("client-1", 1, data)
if err != nil {
    log.Fatal(err)
}
if !resp.Success {
    fmt.Printf("redirecionar para o líder: %s\n", resp.LeaderID)
}
```

### 4. Alterar o cluster em runtime

```go
// Adicionar um nó
node.AddServer("127.0.0.1:9004")

// Remover um nó
node.RemoveServer("127.0.0.1:9004")
```

## Interfaces Plugáveis

### Transport

A interface `transport.Transport` abstrai a comunicação entre nós. A biblioteca inclui o `RPCTransport` (baseado em `net/rpc`), mas qualquer implementação pode ser usada:

```go
type Transport interface {
    SendRequestVote(target types.ServerID, args *types.RequestVoteArgs) (*types.RequestVoteResponse, error)
    SendAppendEntries(target types.ServerID, args *types.AppendEntriesArgs) (*types.AppendEntriesResponse, error)
    SendInstallSnapshot(target types.ServerID, args *types.InstallSnapshotArgs) (*types.InstallSnapshotResponse, error)
    SendApply(target types.ServerID, args *types.ApplyArgs) (*types.ApplyResponse, error)
    SendAddRemoveServer(target types.ServerID, args *types.AddRemoveServerArgs) (*types.AddRemoveServerResponse, error)
    Listen(addr string) error
    Connect(id types.ServerID) error
    Disconnect(id types.ServerID) error
    Close() error
}
```

### Storage

A interface `storage.Storage` abstrai a persistência. A biblioteca inclui o `MemoryStorage` para testes; para produção, implemente persistência em disco:

```go
type Storage interface {
    SaveState(state *types.PersistentState) error
    LoadState() (*types.PersistentState, error)
    SaveSnapshot(snapshot *types.Snapshot) error
    LoadSnapshot() (*types.Snapshot, error)
}
```

## Métricas

O nó expõe métricas de desempenho em tempo real:

```go
m := node.Metrics()

fmt.Println("Apply count:", m.ApplyCount())
fmt.Println("Apply avg:", m.ApplyAvg())
fmt.Println("Apply p99:", m.Percentile(99))
fmt.Println("Elections:", m.ElectionCount())
fmt.Println("Committed entries:", m.CommittedEntries())
```

## Testes

```bash
go test ./... -v
```

A suíte inclui 21 testes cobrindo:

- Eleição de líder (3 testes)
- Replicação de log (4 testes)
- Snapshots (3 testes)
- Mudança de configuração (2 testes)
- Integração TCP (2 testes)
- Persistência e recuperação (1 teste)
- Cenários (leader crash, follower reject) (3 testes)
- Métricas (3 testes)

## Exemplo Completo

O diretório [`examples/kv-store`](examples/kv-store) contém um key-value store distribuído completo. Para executar:

```bash
# Terminal 1
go run ./examples/kv-store -id 127.0.0.1:9001 -peers 127.0.0.1:9002,127.0.0.1:9003

# Terminal 2
go run ./examples/kv-store -id 127.0.0.1:9002 -peers 127.0.0.1:9001,127.0.0.1:9003

# Terminal 3
go run ./examples/kv-store -id 127.0.0.1:9003 -peers 127.0.0.1:9001,127.0.0.1:9002
```

Após a eleição do líder, use os comandos no terminal do líder:

```
SET nome Henrique
GET nome
DEL nome
```

## Configuração

| Parâmetro | Default | Descrição |
|-----------|---------|-----------|
| `HeartbeatMs` | 20 | Intervalo de heartbeat do líder (ms) |
| `ElectionMinMs` | 150 | Timeout mínimo de eleição (ms) |
| `ElectionMaxMs` | 300 | Timeout máximo de eleição (ms) |

## Referências

- [Ongaro, D. & Ousterhout, J. (2014). In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)
- [The Raft Consensus Algorithm](https://raft.github.io/)

## Licença

Este projeto foi desenvolvido para fins acadêmicos como parte do TCC II do curso de Ciência da Computação da Universidade Presbiteriana Mackenzie.
