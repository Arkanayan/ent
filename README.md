# ent

**ent** — Yet another BitTorrent client, written in Rust  

A Rust-based BitTorrent client built for learning, experimentation, and extensibility. This project aims to implement core BitTorrent functionality with a clean and modular design.

---

## Table of Contents

- [Features](#features)  
- [Architecture & Design](#architecture--design)  
- [Getting Started](#getting-started)  
  - [Prerequisites](#prerequisites)  
  - [Building](#building)  
  - [Usage](#usage)  
- [Supported Protocols & Extensions](#supported-protocols--extensions)  
- [Roadmap & TODOs](#roadmap--todos)  
- [License](#license)  

---

## Features

- Parse `.torrent` files and magnet links  
- Connect to HTTP trackers  
- Peer discovery and peer protocol support  
- Download pieces concurrently  
- SHA-1 integrity checks  
- (Planned) DHT, PEX, Local Peer Discovery  
- (Planned) Seeding and upload  

---

## Architecture & Design

- **Torrent Parser**: Parses `.torrent` metadata using bencode  
- **Tracker Client**: Communicates with trackers to fetch peer lists  
- **Peer Manager**: Manages peer connections, handshakes, and data exchange  
- **Piece Manager**: Manages piece requests, validation, and storage  
- **Concurrency**: Async tasks via tokio
- **I/O**: Disk write/read management for downloaded pieces  

---

## Getting Started

### Prerequisites

- [Rust](https://www.rust-lang.org/tools/install) (latest stable)

### Building

```bash
# Clone the repo
git clone https://github.com/Arkanayan/ent.git
cd ent

# Build in debug mode
cargo build

# Or build in release mode
cargo build --release
```

### Usage
```bash
# Run debug mode
cargo run

# Or run in release mode
cargo run --release
```

### Supported Protocols & Extensions
- BEP-3: The BitTorrent Protocol
- BEP-9: Magnet URI support (planned)
- BEP-5: DHT Protocol (planned)
- BEP-10: Extension Protocol (planned)
- Local Peer Discovery (planned)
- IPv6 / uTP support (planned)

## Roadmap & TODOs
- [ ] Magnet link support
- [ ] DHT for trackerless peer discovery
- [ ] Piece prioritization strategies
- [ ] Upload/seeding support
- [ ] Tests and CI
- [ ] Better logging and error handling

## License
MIT License

## Acknowledgements
- [BitTorrent Specification (BEP-3)](https://www.bittorrent.org/beps/bep_0003.html)
- [libtorrent](https://github.com/arvidn/libtorrent)
- [cratetorrent](https://github.com/vimpunk/cratetorrent)
- Rust community & crates ecosystem
