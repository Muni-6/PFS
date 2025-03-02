# Parallel File System (PFS) Using gRPC

## Summary
The main goal of this project is to implement a simple **Parallel File System (PFS)** across multiple servers using **gRPC** as the communication mechanism.

## Overview
The project involves multiple C++ programs where each program (client) may run on different physical machines and perform read/write operations to files using the parallel file system (PFS). The PFS ensures synchronization of operations across multiple clients while providing **sequential consistency**. The file data will be stored on **file servers**, with each server holding portions of a file in a **striped** manner.

---
## Parallel File System (PFS) Architecture and Components
The PFS consists of the following components:
1. **Metadata Server**
2. **File Server(s)**
3. **Client(s) with Client Cache**

PFS is designed as a **server-client architecture**:
- Files are **striped** across multiple file servers.
- Clients interact **directly** with the **metadata server** and **file servers**.
- The **metadata server** does not participate in actual read/write operations but provides metadata information.
- Clients fetch metadata from the **metadata server** and then directly communicate with the **file servers** for reading/writing data.
- There are **no direct connections** between the metadata server and file servers.
- All communications are handled using **gRPC**.

---
## Component 1: Metadata Server
The **metadata server** stores and maintains metadata for all files. The PFS follows a flat file system with only a **root directory** (no subdirectories). Metadata stored per file includes:
1. **File name**
2. **File size**
3. **File creation time** (ctime)
4. **Last file close time** (mtime)
5. **File recipe** (striping details)
6. **Other metadata** (as needed)

A given file in PFS is striped across multiple file servers. The **file recipe** defines how the file is distributed across file servers.

### **Example File Striping**
| Example Filename | Current File Size | Stripe Config | File Location | Notes |
|-----------------|------------------|--------------|--------------|-------|
| abc.txt | 5000 bytes | STRIPE_BLOCKS=2, stripe_width=3, PFS_BLOCK_SIZE=512 | Byte 0-1023: Server 0, Byte 1024-2047: Server 1, etc. | File split across 3 servers in 2-block granularity |
| def.jpg | 3200 bytes | STRIPE_BLOCKS=3, stripe_width=2, PFS_BLOCK_SIZE=512 | Byte 0-1535: Server 0, Byte 1536-3071: Server 1, etc. | File split across 2 servers in 3-block granularity |

### **Metadata Updates**
- **File creation time** is updated when `pfs_create()` is called.
- **Last file close time** is updated on `pfs_close()`.
- Metadata is stored using a struct definition in `pfs_fstat()`.

---
## Component 2: File Server
- **File servers store raw file data in disk (not memory).**
- The number of file servers a file spans depends on **stripe_width**.
- Each file may be distributed across multiple servers.
- The file server **never communicates** with the metadata server.
- The underlying storage system is **NFS (Network File System)**.

---
## Component 3: Client (PFS API)
Clients use the following API to interact with PFS:

### **Initialization and Cleanup**
```cpp
int pfs_initialize();
int pfs_finish(int client_id);
```

### **File Operations**
```cpp
int pfs_create(const char *filename, int stripe_width);
int pfs_open(const char *filename, int mode);
int pfs_read(int fd, void *buf, size_t num_bytes, off_t offset);
int pfs_write(int fd, const void *buf, size_t num_bytes, off_t offset);
int pfs_close(int fd);
int pfs_delete(const char *filename);
int pfs_fstat(int fd, struct pfs_metadata *meta_data);
```

### **Execution Statistics**
```cpp
int pfs_execstat(struct pfs_execstat *execstat_data);
```
- `pfs_execstat()` returns statistics such as read/write hits, evictions, and invalidations.

---
## **Consistency and Lock Management in Metadata Server**
- **Sequential consistency** is enforced via **token management**.
- Tokens (read/write locks) are handed out by the **metadata server**.
- Tokens are required for **reads/writes**.
- Conflicting operations are serialized, but **non-conflicting** operations can be concurrent.

Example:
- **Client 0** has a **write token** for `bytes 500-800`.
- **Client 1** requests a **write token** for `bytes 600-700`.
- Metadata server revokes **Client 0’s** token for `600-700` before granting it to **Client 1**.

All tokens are **released** when `pfs_close()` is called.

---
## **Client Cache Management**
- Clients maintain a **local cache** of size `CLIENT_CACHE_BLOCKS * PFS_BLOCK_SIZE`.
- Blocks in cache are evicted when space runs out.
- **Shared blocks** require careful handling to maintain consistency.
- **LRU (Least Recently Used)** policy is used to manage cache space.

---
## **gRPC, Protobuf, and Code Structure**
### **gRPC**
- gRPC is used for **server-client** communication.
- It provides **high-performance remote procedure calls (RPCs)**.
- API documentation: [grpc.io](https://grpc.io)

### **Protobuf**
- Used for defining structured data communication.
- The `protoc` compiler generates `.pb.h` and `.grpc.pb.h` files for RPC communication.
- API documentation: [protobuf.dev](https://protobuf.dev)

### **Project Structure**
```
- pfs_common/   # Utility functions
- pfs_config.hpp  # Configuration settings
- pfs_api.hpp/.cpp  # Client API implementation
- pfs_server/   # File server implementation
- pfs_metadata/ # Metadata server implementation
```

---
## **Execution Statistics to Collect**
Clients should track:
- `num_read_hits`, `num_write_hits`
- `num_evictions`, `num_writebacks`
- `num_invalidations`
- `num_close_writebacks`, `num_close_evictions`

These will be returned in `pfs_execstat()`.

---
## **Conclusion**
This project implements a **Parallel File System** with:
- **gRPC-based communication**
- **Metadata and File Servers**
- **Client-side caching and consistency mechanisms**


