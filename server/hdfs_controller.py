# =============================================================
#  hdfs_controller.py  —  HDFS-Style Block Manager
#  Location: cloud_file_transfer/server/hdfs_controller.py
# =============================================================

import os
import json


class HDFSController:
    """
    Simulates HDFS (Hadoop Distributed File System) behavior:

    Real HDFS concepts implemented here:
      - NameNode  : our metadata JSON files track block locations
      - DataNodes : local folders (datanode1/, datanode2/, datanode3/)
      - Blocks    : files split into fixed-size chunks (default 128 KB)
      - Replication : each block stored on N datanodes for fault tolerance

    On-disk block format:
      [16 bytes nonce] + [16 bytes GCM tag] + [N bytes ciphertext]
    """

    def __init__(self, storage_dir: str, metadata_dir: str,
                 block_size: int, replication: int):
        self.storage_dir  = storage_dir
        self.metadata_dir = metadata_dir
        self.block_size   = block_size
        self.replication  = replication

        # Auto-detect all datanode subfolders
        self.datanodes = sorted([
            os.path.join(storage_dir, d)
            for d in os.listdir(storage_dir)
            if os.path.isdir(os.path.join(storage_dir, d))
        ])

        print(f"[HDFS] NameNode started")
        print(f"[HDFS] Detected {len(self.datanodes)} DataNode(s):")
        for dn in self.datanodes:
            print(f"       → {os.path.basename(dn)}")
        print(f"[HDFS] Block size : {self.block_size // 1024} KB")
        print(f"[HDFS] Replication: {self.replication}x")

    # ----------------------------------------------------------
    #  Block Operations
    # ----------------------------------------------------------

    def split_into_blocks(self, data: bytes) -> list:
        """
        Split raw file bytes into fixed-size blocks.

        Example: 350 KB file with 128 KB block size
          → Block 0: bytes 0      to 131071   (128 KB)
          → Block 1: bytes 131072 to 262143   (128 KB)
          → Block 2: bytes 262144 to 357399   (95 KB — last block)

        Args:
            data (bytes) : complete file contents

        Returns:
            list of bytes : each element is one block
        """
        blocks = []
        for i in range(0, len(data), self.block_size):
            blocks.append(data[i : i + self.block_size])

        print(f"[HDFS] File split into {len(blocks)} block(s) "
              f"({self.block_size // 1024} KB each)")
        return blocks

    def store_block(self, file_id: str, block_idx: int,
                    enc_data: bytes, nonce: bytes, tag: bytes) -> list:
        """
        Write an encrypted block to N DataNode folders (replication).

        File naming:  <file_id>_block<NNNN>.enc
        File content: nonce (16B) + tag (16B) + ciphertext

        Args:
            file_id   (str)   : unique identifier for the file
            block_idx (int)   : 0-based block number
            enc_data  (bytes) : encrypted block ciphertext
            nonce     (bytes) : 16-byte AES-GCM nonce
            tag       (bytes) : 16-byte AES-GCM auth tag

        Returns:
            list of dicts : [{'node': 'datanode1', 'path': '/full/path'}]
        """
        locations = []
        rep_count = min(self.replication, len(self.datanodes))

        for r in range(rep_count):
            # Round-robin across datanodes
            node_path  = self.datanodes[r % len(self.datanodes)]
            node_name  = os.path.basename(node_path)
            block_file = f"{file_id}_block{block_idx:04d}.enc"
            full_path  = os.path.join(node_path, block_file)

            # Write: nonce + tag + ciphertext (all binary)
            with open(full_path, "wb") as f:
                f.write(nonce + tag + enc_data)

            locations.append({"node": node_name, "path": full_path})
            print(f"[HDFS] Block {block_idx:03d} → {node_name}  "
                  f"({len(enc_data):,} bytes encrypted)")

        return locations

    def fetch_block(self, file_id: str, block_idx: int,
                    locations: list) -> bytes:
        """
        Read the ciphertext of a block from the first available DataNode.
        Tries replicas in order — provides fault tolerance.

        Returns:
            bytes : raw ciphertext (without nonce/tag prefix)
            None  : if all replicas are unavailable
        """
        for loc in locations:
            path = loc["path"]
            if os.path.exists(path):
                with open(path, "rb") as f:
                    raw = f.read()
                # Layout: [0:16] = nonce, [16:32] = tag, [32:] = ciphertext
                ciphertext = raw[32:]
                print(f"[HDFS] Block {block_idx:03d} ← {loc['node']}  "
                      f"({len(ciphertext):,} bytes)")
                return ciphertext

        print(f"[HDFS] ERROR: Block {block_idx} not found on any DataNode!")
        return None

    def delete_blocks(self, file_id: str, block_map: list):
        """Remove all encrypted .enc files for a given file."""
        deleted = 0
        for block_info in block_map:
            for loc in block_info["locations"]:
                path = loc["path"]
                if os.path.exists(path):
                    os.remove(path)
                    deleted += 1
                    print(f"[HDFS] Deleted: {os.path.basename(path)}")
        print(f"[HDFS] Removed {deleted} block file(s)")

    # ----------------------------------------------------------
    #  Metadata Operations (NameNode duties)
    # ----------------------------------------------------------

    def save_metadata(self, file_id: str, metadata: dict):
        """
        Persist file metadata (block map) as JSON.
        This is what a real HDFS NameNode keeps in memory / on disk.
        """
        path = os.path.join(self.metadata_dir, f"{file_id}.json")
        with open(path, "w") as f:
            json.dump(metadata, f, indent=2)
        print(f"[HDFS] Metadata saved: {file_id}.json")

    def load_metadata(self, file_id: str) -> dict:
        """Load file metadata from disk. Returns None if not found."""
        path = os.path.join(self.metadata_dir, f"{file_id}.json")
        if not os.path.exists(path):
            return None
        with open(path, "r") as f:
            return json.load(f)

    def delete_metadata(self, file_id: str):
        """Delete the metadata JSON file for a given file."""
        path = os.path.join(self.metadata_dir, f"{file_id}.json")
        if os.path.exists(path):
            os.remove(path)
            print(f"[HDFS] Metadata deleted: {file_id}.json")

    # ----------------------------------------------------------
    #  Node Status
    # ----------------------------------------------------------

    def get_node_status(self) -> list:
        """
        Collect storage statistics from each DataNode folder.

        Returns:
            list of dicts with keys:
              node          : folder name (e.g. 'datanode1')
              blocks_stored : number of .enc files
              bytes_used    : total bytes on that node
              status        : 'ONLINE' (always, in this simulation)
        """
        status = []
        for node_path in self.datanodes:
            name  = os.path.basename(node_path)
            files = [f for f in os.listdir(node_path)
                     if f.endswith(".enc")]
            total = sum(
                os.path.getsize(os.path.join(node_path, f))
                for f in files
            )
            status.append({
                "node"          : name,
                "blocks_stored" : len(files),
                "bytes_used"    : total,
                "status"        : "ONLINE"
            })
        return status
