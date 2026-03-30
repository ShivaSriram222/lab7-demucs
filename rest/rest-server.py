#!/usr/bin/env python3

import os
import sys
import json
import hashlib
import base64
import jsonpickle
import redis
import requests as req_lib
from flask import Flask, request, Response, send_file
from minio import Minio
from minio.error import S3Error
import io

app = Flask(__name__)

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

def log_info(msg):
    try:
        r = get_redis()
        r.lpush("logging", f"[REST] {msg}")
    except Exception:
        pass
    print(f"[REST] {msg}", flush=True)

@app.route('/', methods=['GET'])
def hello():
    return '<h1>Music Separation Server</h1><p>Use a valid endpoint</p>'

@app.route('/apiv1/separate', methods=['POST'])
def separate():
    try:
        body = request.get_json(force=True)
        if body is None:
            body = jsonpickle.decode(request.data)

        mp3_b64  = body.get("mp3")
        model    = body.get("model", "htdemucs")
        callback = body.get("callback", None)

        if not mp3_b64:
            return Response(json.dumps({"error": "missing mp3 field"}),
                            status=400, mimetype="application/json")

        mp3_bytes = base64.b64decode(mp3_b64)
        songhash  = hashlib.sha256(mp3_bytes).hexdigest()

        minio_client = get_minio()
        ensure_buckets(minio_client)
        minio_client.put_object(
            QUEUE_BUCKET,
            f"{songhash}.mp3",
            io.BytesIO(mp3_bytes),
            length=len(mp3_bytes),
            content_type="audio/mpeg"
        )

        work_item = {"songhash": songhash, "model": model, "callback": callback}
        r = get_redis()
        r.lpush("toWorker", json.dumps(work_item))
        log_info(f"Enqueued song {songhash[:12]}... model={model}")

        return Response(
            json.dumps({"hash": songhash, "reason": "Song enqueued for separation"}),
            status=200, mimetype="application/json"
        )
    except Exception as e:
        log_info(f"ERROR in /apiv1/separate: {e}")
        return Response(json.dumps({"error": str(e)}), status=500, mimetype="application/json")

@app.route('/apiv1/queue', methods=['GET'])
def get_queue():
    try:
        r = get_redis()
        items = r.lrange("toWorker", 0, -1)
        queue = []
        for item in items:
            try:
                work = json.loads(item)
                queue.append(work.get("songhash", str(item)))
            except Exception:
                queue.append(str(item))
        return Response(json.dumps({"queue": queue}), status=200, mimetype="application/json")
    except Exception as e:
        return Response(json.dumps({"error": str(e)}), status=500, mimetype="application/json")

@app.route('/apiv1/track/<songhash>/<track>', methods=['GET'])
def get_track(songhash, track):
    try:
        if not track.endswith(".mp3"):
            track = track + ".mp3"
        object_name = f"{songhash}-{track}"
        minio_client = get_minio()
        data = minio_client.get_object(OUTPUT_BUCKET, object_name)
        mp3_data = data.read()
        return Response(
            mp3_data, status=200, mimetype="audio/mpeg",
            headers={"Content-Disposition": f"attachment; filename={track}"}
        )
    except S3Error as e:
        if e.code == "NoSuchKey":
            return Response(
                json.dumps({"error": f"Track {track} for {songhash} not found"}),
                status=404, mimetype="application/json"
            )
        return Response(json.dumps({"error": str(e)}), status=500, mimetype="application/json")
    except Exception as e:
        return Response(json.dumps({"error": str(e)}), status=500, mimetype="application/json")

@app.route('/apiv1/remove/<songhash>/<track>', methods=['GET', 'DELETE'])
def remove_track(songhash, track):
    try:
        if not track.endswith(".mp3"):
            track = track + ".mp3"
        object_name = f"{songhash}-{track}"
        minio_client = get_minio()
        minio_client.remove_object(OUTPUT_BUCKET, object_name)
        return Response(json.dumps({"removed": object_name}), status=200, mimetype="application/json")
    except Exception as e:
        return Response(json.dumps({"error": str(e)}), status=500, mimetype="application/json")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
