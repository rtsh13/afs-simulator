## Architecture Summary

```
┌─────────────────────────────────────────────────────┐
│                 Client Applications                  │
│            (Workers reading/writing files)           │
└──────────────┬──────────────┬──────────────┬────────┘
               │              │              │
            RPC│           RPC│           RPC│
               │              │              │
┌──────────────▼──────┐ ┌─────▼──────┐ ┌────▼─────────┐
│ File Server 1       │ │ File Server│ │ File Server 3│
│    (Primary)        │◄─┤     2      │◄─┤   (Backup)   │
│                     ├─►│  (Backup)  ├─►│              │
└─────────────────────┘ └────────────┘ └──────────────┘
         │                                      │
         │ Replication Log                     │
         │                                      │
         └──────────────┬───────────────────────┘
                        │
                        │
          ┌─────────────▼────────────┐
          │     Coordinator          │
          │  (Task Distribution)     │
          │  (Snapshot Orchestrator) │
          └──────┬───────────────┬───┘
                 │               │
        Socket   │               │   Socket
                 │               │
        ┌────────▼──┐       ┌───▼────────┐
        │  Worker 1 │  ...  │  Worker N  │
        │  (Compute)│       │  (Compute) │
        └───────────┘       └────────────┘
```

