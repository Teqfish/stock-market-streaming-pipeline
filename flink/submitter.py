import json
import subprocess

from flask import Flask, jsonify, request


app = Flask(__name__)

FLINK_BIN = "/opt/flink/bin/flink"
JOBMANAGER = "jobmanager:8081"
BACKFILL_JOB = "/opt/flink/jobs/backfill.py"
PROCESSING_FILE = "/opt/flink/jobs/processing.py"


@app.get("/health")
def health():
    return jsonify({"status": "ok"})


@app.post("/backfills")
def run_backfill():
    payload = request.get_json(silent=True)

    if not isinstance(payload, dict):
        return jsonify({"error": "Request body must be a JSON object"}), 400

    required_fields = {
        "symbols",
        "processing_start",
        "processing_end",
        "requested_start",
        "requested_end",
        "start_offsets",
        "end_offsets",
    }

    missing = required_fields - payload.keys()

    if missing:
        return jsonify(
            {"error": f"Missing required fields: {', '.join(sorted(missing))}"}
        ), 400

    symbols = payload["symbols"]
    processing_start = payload["processing_start"]
    processing_end = payload["processing_end"]
    requested_start = payload["requested_start"]
    requested_end = payload["requested_end"]
    start_offsets = payload["start_offsets"]
    end_offsets = payload["end_offsets"]

    if not isinstance(symbols, list) or not symbols:
        return jsonify({"error": "symbols must be a non-empty list"}), 400

    if not all(isinstance(symbol, str) and symbol for symbol in symbols):
        return jsonify({"error": "symbols must contain non-empty strings"}), 400

    timestamps = {
        "processing_start": processing_start,
        "processing_end": processing_end,
        "requested_start": requested_start,
        "requested_end": requested_end,
    }

    if not all(
        isinstance(value, str)
        for value in timestamps.values()
    ):
        return jsonify(
            {
                "error": (
                    "processing_start, processing_end, "
                    "requested_start and requested_end "
                    "must be strings"
                )
            }
        ), 400

    if not isinstance(start_offsets, dict) or not isinstance(end_offsets, dict):
        return jsonify(
            {"error": "start_offsets and end_offsets must be objects"}
        ), 400

    command = [
        FLINK_BIN,
        "run",
        "--jobmanager",
        JOBMANAGER,
        "--python",
        BACKFILL_JOB,
        "--pyFiles",
        PROCESSING_FILE,
        "--symbols",
        ",".join(symbols),
        "--processing-start",
        processing_start,
        "--processing-end",
        processing_end,
        "--requested-start",
        requested_start,
        "--requested-end",
        requested_end,
        "--start-offsets",
        json.dumps(start_offsets),
        "--end-offsets",
        json.dumps(end_offsets),
    ]

    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        return jsonify(
            {
                "status": "failed",
                "returncode": exc.returncode,
                "stdout": exc.stdout,
                "stderr": exc.stderr,
            }
        ), 500

    return jsonify(
        {
            "status": "finished",
            "stdout": result.stdout,
            "stderr": result.stderr,
        }
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8090)
