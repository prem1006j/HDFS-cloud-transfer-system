# =============================================================
#  app.py  —  Cloud Controller / Flask REST API Server
#  Location: cloud_file_transfer/server/app.py
#
#  Run with:  python3 app.py
#  Access at: http://localhost:5000         (same machine)
#             http://<YOUR_LAN_IP>:5000     (any device on LAN)
# =============================================================

import os
import json
import uuid
import time
import io

from flask import Flask, request, jsonify, send_file, render_template
from flask_cors import CORS
from werkzeug.utils import secure_filename

from hdfs_controller import HDFSController
from crypto_utils    import CryptoUtils


# ──────────────────────────────────────────────────────────────
#  App Setup
# ──────────────────────────────────────────────────────────────

app = Flask(
    __name__,
    template_folder="../client/templates",   # index.html lives here
    static_folder="../client/static"         # app.js lives here
)
CORS(app)   # Allow requests from any origin on the LAN


# ──────────────────────────────────────────────────────────────
#  Configuration
# ──────────────────────────────────────────────────────────────

BASE_DIR      = os.path.dirname(os.path.abspath(__file__))
STORAGE_DIR   = os.path.join(BASE_DIR, "storage")
KEYS_DIR      = os.path.join(BASE_DIR, "keys")
METADATA_DIR  = os.path.join(BASE_DIR, "metadata")

BLOCK_SIZE    = 128 * 1024   # 128 KB per block (same as HDFS default concept)
REPLICATION   = 3            # Write each block to 3 DataNodes

# Initialise sub-systems
hdfs   = HDFSController(STORAGE_DIR, METADATA_DIR, BLOCK_SIZE, REPLICATION)
crypto = CryptoUtils(KEYS_DIR)


# ──────────────────────────────────────────────────────────────
#  Helper
# ──────────────────────────────────────────────────────────────

def human_bytes(n: int) -> str:
    """Convert byte count to human-readable string."""
    if n < 1024:
        return f"{n} B"
    if n < 1024 ** 2:
        return f"{n / 1024:.1f} KB"
    return f"{n / 1024 ** 2:.2f} MB"


# ──────────────────────────────────────────────────────────────
#  Route 0  —  Serve the Browser UI
# ──────────────────────────────────────────────────────────────

@app.route("/")
def index():
    """Serve the main HTML page (SaaS frontend)."""
    return render_template("index.html")


# ──────────────────────────────────────────────────────────────
#  Route 1  —  Upload
#  POST /api/upload
#  Form field: file  (multipart/form-data)
# ──────────────────────────────────────────────────────────────

@app.route("/api/upload", methods=["POST"])
def upload_file():
    """
    Full upload pipeline:
      1. Read incoming file bytes
      2. Generate unique AES-256 key for this file
      3. Split file into fixed-size blocks
      4. Encrypt each block (AES-256-GCM)
      5. Store encrypted blocks on DataNode folders
      6. Save metadata (block map) as JSON
      7. Return file_id + encryption key to client
    """

    # ── Validate input ────────────────────────────────────────
    if "file" not in request.files:
        return jsonify({"error": "No file field in request"}), 400

    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "Empty filename"}), 400

    # ── Read file ─────────────────────────────────────────────
    filename  = secure_filename(file.filename)
    file_id   = str(uuid.uuid4())          # e.g. "a3f9b2c1-..."
    file_data = file.read()

    print(f"\n[API] UPLOAD  → {filename}  ({human_bytes(len(file_data))})")
    print(f"[API] File ID : {file_id}")

    # ── Generate AES key ──────────────────────────────────────
    key, key_hex = crypto.generate_key()
    crypto.save_key(file_id, key)
    print(f"[API] AES key generated and saved")

    # ── Split into blocks ─────────────────────────────────────
    blocks = hdfs.split_into_blocks(file_data)

    # ── Encrypt + store each block ────────────────────────────
    block_map = []
    for i, block in enumerate(blocks):
        enc_block, nonce, tag = crypto.encrypt_block(block, key)
        locations = hdfs.store_block(file_id, i, enc_block, nonce, tag)

        block_map.append({
            "block_index" : i,
            "block_size"  : len(block),
            "nonce"       : nonce.hex(),   # store as hex string in JSON
            "tag"         : tag.hex(),
            "locations"   : locations
        })

    # ── Save metadata ─────────────────────────────────────────
    metadata = {
        "file_id"    : file_id,
        "filename"   : filename,
        "size"       : len(file_data),
        "blocks"     : len(blocks),
        "block_size" : BLOCK_SIZE,
        "uploaded"   : time.strftime("%Y-%m-%d %H:%M:%S"),
        "block_map"  : block_map
    }
    hdfs.save_metadata(file_id, metadata)

    print(f"[API] Upload complete: {len(blocks)} blocks stored\n")

    # Return key to client — client MUST save it for download
    return jsonify({
        "success"  : True,
        "file_id"  : file_id,
        "filename" : filename,
        "size"     : len(file_data),
        "blocks"   : len(blocks),
        "key"      : key_hex
    }), 200


# ──────────────────────────────────────────────────────────────
#  Route 2  —  Download
#  GET /api/download/<file_id>
# ──────────────────────────────────────────────────────────────

@app.route("/api/download/<file_id>", methods=["GET"])
def download_file(file_id):
    """
    Full download pipeline:
      1. Load block-map metadata
      2. Load AES key from disk
      3. For each block: fetch encrypted bytes from DataNode
      4. Decrypt block (AES-256-GCM) — verifies auth tag
      5. Reassemble all blocks into original file bytes
      6. Stream file back to client as attachment
    """

    print(f"\n[API] DOWNLOAD ← {file_id}")

    # ── Load metadata ─────────────────────────────────────────
    metadata = hdfs.load_metadata(file_id)
    if not metadata:
        return jsonify({"error": "File not found — unknown file_id"}), 404

    # ── Load AES key ──────────────────────────────────────────
    key = crypto.load_key(file_id)
    if not key:
        return jsonify({"error": "Encryption key not found on server"}), 404

    # ── Fetch + decrypt each block ────────────────────────────
    file_data = b""
    for block_info in metadata["block_map"]:
        idx   = block_info["block_index"]
        nonce = bytes.fromhex(block_info["nonce"])
        tag   = bytes.fromhex(block_info["tag"])

        # Fetch ciphertext from DataNode (tries replicas on failure)
        enc_block = hdfs.fetch_block(file_id, idx, block_info["locations"])
        if enc_block is None:
            return jsonify({
                "error": f"Block {idx} unavailable on all DataNodes"
            }), 500

        # Decrypt — raises ValueError if data is tampered
        try:
            dec_block = crypto.decrypt_block(enc_block, key, nonce, tag)
        except ValueError:
            return jsonify({
                "error": f"Block {idx} authentication failed — data corrupted"
            }), 500

        file_data += dec_block

    print(f"[API] Reassembled {human_bytes(len(file_data))}  "
          f"from {len(metadata['block_map'])} blocks")

    # ── Stream file to client ─────────────────────────────────
    return send_file(
        io.BytesIO(file_data),
        download_name=metadata["filename"],
        as_attachment=True,
        mimetype="application/octet-stream"
    )


# ──────────────────────────────────────────────────────────────
#  Route 3  —  List Files
#  GET /api/files
# ──────────────────────────────────────────────────────────────

@app.route("/api/files", methods=["GET"])
def list_files():
    """Return a list of all uploaded files with their metadata."""
    files = []
    for fname in os.listdir(METADATA_DIR):
        if fname.endswith(".json"):
            fid  = fname.replace(".json", "")
            meta = hdfs.load_metadata(fid)
            if meta:
                files.append({
                    "file_id"  : meta["file_id"],
                    "filename" : meta["filename"],
                    "size"     : meta["size"],
                    "blocks"   : meta["blocks"],
                    "uploaded" : meta["uploaded"]
                })

    # Sort newest first
    files.sort(key=lambda x: x["uploaded"], reverse=True)
    return jsonify({"files": files, "total": len(files)}), 200


# ──────────────────────────────────────────────────────────────
#  Route 4  —  Delete File
#  DELETE /api/delete/<file_id>
# ──────────────────────────────────────────────────────────────

@app.route("/api/delete/<file_id>", methods=["DELETE"])
def delete_file(file_id):
    """
    Permanently delete:
      - All encrypted block files from every DataNode
      - The AES key file
      - The metadata JSON
    """
    print(f"\n[API] DELETE  {file_id}")

    metadata = hdfs.load_metadata(file_id)
    if not metadata:
        return jsonify({"error": "File not found"}), 404

    hdfs.delete_blocks(file_id, metadata["block_map"])
    crypto.delete_key(file_id)
    hdfs.delete_metadata(file_id)

    print(f"[API] Deleted: {metadata['filename']}\n")
    return jsonify({
        "success"  : True,
        "message"  : f"{metadata['filename']} deleted successfully"
    }), 200


# ──────────────────────────────────────────────────────────────
#  Route 5  —  Node Status
#  GET /api/status
# ──────────────────────────────────────────────────────────────

@app.route("/api/status", methods=["GET"])
def node_status():
    """Return storage statistics for each DataNode."""
    return jsonify(hdfs.get_node_status()), 200


# ──────────────────────────────────────────────────────────────
#  Route 6  —  Health Check
#  GET /api/health
# ──────────────────────────────────────────────────────────────

@app.route("/api/health", methods=["GET"])
def health():
    """Simple ping endpoint."""
    return jsonify({
        "status"      : "ok",
        "server"      : "CloudFS HDFS Controller",
        "version"     : "1.0",
        "block_size"  : BLOCK_SIZE,
        "replication" : REPLICATION,
        "datanodes"   : len(hdfs.datanodes)
    }), 200


# ──────────────────────────────────────────────────────────────
#  Entry Point
# ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("  CloudFS — HDFS File Transfer System")
    print("=" * 60)
    print(f"  Storage   : {STORAGE_DIR}")
    print(f"  Keys      : {KEYS_DIR}")
    print(f"  Metadata  : {METADATA_DIR}")
    print(f"  Block size: {BLOCK_SIZE // 1024} KB")
    print(f"  Replication: {REPLICATION}x")
    print("=" * 60)
    print("  Open in browser:")
    print("  → http://localhost:5000          (this machine)")
    print("  → http://<LAN_IP>:5000           (other devices)")
    print("=" * 60)

    # host='0.0.0.0' = listen on all interfaces including LAN
    app.run(host="0.0.0.0", port=5000, debug=True)
