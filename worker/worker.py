#!/usr/bin/env python3

import os
import sys
import json
import time
import redis
import requests
from minio import Minio
from minio.error import S3Error
import io

REDIS_HOST  = os.getenv("REDIS_HOST",  "localhost")
REDIS_PORT  = int(os.getenv("REDIS_PORT", "6379"))
MINIO_HOST  = os.getenv("MINIO_HOST",  "localhost:9000")
MINIO_USER  = os.getenv("MINIO_USER",  "rootuser")
MINIO_PASS  = os.getenv("MINIO_PASS",  "rootpass123")

QUEUE_BUCKET  = "queue"
OUTPUT_BUCKET = "output"

def get_redis():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)

def get_minio():
    return Minio(MINIO_HOST, access_key=MINIO_USER, secret_key=MINIO_PASS, secure=False)

def ensure_buckets(client):
    for bucket in [QUEUE_BUCKET, OUTPUT_BUCKET]:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)

def log_info(r_client, msg):
    print(f"[WORKER] {msg}", flush=True)
    try:
        r_client.lpush("logging", f"[WORKER] {msg}")
    except Exception:
        pass

def log_debug(r_client, msg):
    print(f"[WORKER DEBUG] {msg}", flush=True)
    try:
        r_client.lpush("logging", f"[WORKER DEBUG] {msg}")
    except Exception:
        pass

def main():
    print("[WORKER] Starting up...", flush=True)

    r = None
    while r is None:
        try:
            r = get_redis()
            r.ping()
            print("[WORKER] Connected to Redis", flush=True)
        except Exception as e:
            print(f"[WORKER] Waiting for Redis: {e}", flush=True)
            time.sleep(2)
            r = None

    minio_client = None
    while minio_client is None:
        try:
            minio_client = get_minio()
            ensure_buckets(minio_client)
            print("[WORKER] Connected to MinIO", flush=True)
        except Exception as e:
            print(f"[WORKER] Waiting for MinIO: {e}", flush=True)
            time.sleep(2)
            minio_client = None

    log_info(r, "Worker ready, listening on toWorker queue...")

    while True:
        try:
            result = r.brpop("toWorker", timeout=10)
            if result is None:
                continue

            _, raw = result
            work = json.loads(raw)
            songhash = work["songhash"]
            model    = work.get("model", "mdx_extra_q")
            callback = work.get("callback", None)

            # Force mdx_extra_q if htdemucs requested (not supported in this container)
            if model == "htdemucs":
                model = "mdx_extra_q"

            log_info(r, f"Received job: songhash={songhash[:12]}... model={model}")

            input_path = f"/tmp/{songhash}.mp3"
            output_dir = f"/tmp/output"
            os.makedirs(output_dir, exist_ok=True)

            try:
                minio_client = get_minio()
                data = minio_client.get_object(QUEUE_BUCKET, f"{songhash}.mp3")
                with open(input_path, "wb") as f:
                    f.write(data.read())
                log_info(r, f"Downloaded {songhash[:12]}.mp3 from MinIO")
            except Exception as e:
                log_info(r, f"ERROR downloading from MinIO: {e}")
                continue

            cmd = (
                f"python3 -m demucs.separate "
                f"--mp3 "
                f"-n {model} "
                f"--out {output_dir} "
                f"{input_path}"
            )
            log_info(r, f"Running: {cmd}")
            ret = os.system(cmd)

            if ret != 0:
                log_info(r, f"ERROR: demucs returned non-zero exit code {ret}")
                continue

            log_info(r, "DEMUCS separation complete")

            tracks = ["bass", "drums", "vocals", "other"]
            uploaded = []
            track_dir = None

            for root, dirs, files in os.walk(output_dir):
                if os.path.basename(root) == songhash:
                    track_dir = root
                    break

            if track_dir is None:
                log_info(r, f"ERROR: Could not find output directory for {songhash}")
                continue

            log_info(r, f"Found output directory: {track_dir}")

            for part in tracks:
                part_path = os.path.join(track_dir, f"{part}.mp3")
                if not os.path.exists(part_path):
                    log_info(r, f"WARNING: {part_path} not found")
                    continue

                object_name = f"{songhash}-{part}.mp3"
                with open(part_path, "rb") as f:
                    data_bytes = f.read()

                minio_client.put_object(
                    OUTPUT_BUCKET,
                    object_name,
                    io.BytesIO(data_bytes),
                    length=len(data_bytes),
                    content_type="audio/mpeg"
                )
                uploaded.append(object_name)
                log_info(r, f"Uploaded {object_name} to MinIO")

            log_info(r, f"All tracks uploaded for {songhash[:12]}: {uploaded}")

            if callback and isinstance(callback, dict):
                cb_url  = callback.get("url")
                cb_data = callback.get("data", {})
                if cb_url:
                    try:
                        requests.post(cb_url, json={"hash": songhash, **cb_data}, timeout=5)
                        log_info(r, f"Callback fired to {cb_url}")
                    except Exception as e:
                        log_info(r, f"Callback failed (non-fatal): {e}")

            try:
                if os.path.exists(input_path):
                    os.remove(input_path)
                if track_dir and os.path.isdir(track_dir):
                    import shutil
                    shutil.rmtree(track_dir, ignore_errors=True)
            except Exception as e:
                log_debug(r, f"Cleanup warning: {e}")

        except KeyboardInterrupt:
            print("[WORKER] Shutting down", flush=True)
            sys.exit(0)
        except Exception as e:
            print(f"[WORKER] Unexpected error: {e}", flush=True)
            time.sleep(2)

if __name__ == '__main__':
    main()
