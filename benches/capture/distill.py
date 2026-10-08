#!/usr/bin/env python3
"""Distills captured traces (record-proxy.mjs JSON lines) into the workload
profiles the benchmarks replay: benches/workloads/profiles.json.

    benches/capture/distill.py <capture dir> > benches/workloads/profiles.json

Keys are reduced to their shape (prefix and length) and payloads to sizes, so
the profiles carry no project data. Each capture directory holds one
scenario's trace.jsonl; `GET /__phase/<name>/start|end` marker requests split
a trace into phases (cold build, warm build, ...).
"""
import json
import sys
from pathlib import Path

TWIRP = "/twirp/github.actions.results.api.v1.CacheService/"


def load(path):
    with open(path) as trace:
        return [json.loads(line) for line in trace if line.strip()]


def phases(records):
    """Splits a trace at its /__phase markers; a trace without them is one phase."""
    out, current, name = {}, [], "all"
    for record in records:
        url = record["url"]
        if url.startswith("/__phase/"):
            _, _, phase, edge = url.split("/")[:4]
            if edge == "start":
                current, name = [], phase
            else:
                out[name] = current
                current = []
            continue
        current.append(record)
    if current:
        out[name] = current
    return out


def body(text):
    try:
        return json.loads(text or "{}")
    except json.JSONDecodeError:
        return {}


def twirp_calls(records):
    """(method, request, response, record) for every JSON Twirp call."""
    for record in records:
        if TWIRP in record["url"] and record.get("req_headers", {}).get("content-type", "").startswith("application/json"):
            yield record["url"].rsplit("/", 1)[1], body(record.get("req_body")), body(record.get("resp_body")), record


def entries(records):
    """Per phase facts about every key: finalized size, lookup outcomes, and
    whether it was downloaded, in first-seen order."""
    upload_key, download_key, facts = {}, {}, {}

    def fact(key):
        return facts.setdefault(key, {"key": key, "size": None, "lookups": [], "downloaded": False, "created": False})

    for method, request, response, record in twirp_calls(records):
        key = request.get("key", "")
        if method == "GetCacheEntryDownloadURL":
            hit = response.get("ok") is True
            fact(key)["lookups"].append(hit)
            if hit:
                matched = response.get("matched_key", key)
                download_key[response["signed_download_url"].split("/download/")[-1]] = matched
        elif method == "CreateCacheEntry":
            fact(key)["created"] = response.get("ok") is True
            if response.get("ok"):
                upload_key[response["signed_upload_url"].rsplit("/", 1)[-1]] = key
        elif method == "FinalizeCacheEntryUpload":
            fact(key)["size"] = int(request.get("size_bytes", 0))
    for record in records:
        if record["method"] == "GET" and "/download/" in record["url"]:
            key = download_key.get(record["url"].split("/download/")[-1])
            if key is not None:
                fact(key)["downloaded"] = True
                fact(key)["size"] = fact(key)["size"] or record.get("resp_bytes")
    return list(facts.values())


def buildkit(directory):
    """A BuildKit image profile: the cache chain in export order with layer
    sizes, the index size, the layers a warm build downloads, and what a
    source change adds."""
    by_phase = phases(load(directory / "trace.jsonl"))
    sizes = {}
    for records in by_phase.values():
        for entry in entries(records):
            if entry["size"]:
                sizes[entry["key"]] = entry["size"]
    blob = lambda key: key.startswith("buildkit-blob-")
    cold = [entry for entry in entries(by_phase["run1-cold"]) if blob(entry["key"])]
    chain = [entry["key"] for entry in cold]
    index_sizes = [size for key, size in sizes.items() if key.startswith("index-")]
    warm = [entry for entry in entries(by_phase["run2-warm"]) if blob(entry["key"])]
    profile = {
        "source": directory.name,
        "chain": [sizes[key] for key in chain if key in sizes],
        "index_size": max(index_sizes) if index_sizes else 0,
        # Positions in `chain` of the layers a warm build downloads.
        "warm_downloads": [chain.index(entry["key"]) for entry in warm if entry["downloaded"] and entry["key"] in chain],
    }
    change = by_phase.get("run3-change")
    if change is not None:
        changed = [entry for entry in entries(change) if blob(entry["key"])]
        profile["change_new_layers"] = [entry["size"] for entry in changed if entry["created"] and entry["size"]]
        profile["change_downloads"] = [chain.index(entry["key"]) for entry in changed if entry["downloaded"] and entry["key"] in chain]
    return profile


def main():
    root = Path(sys.argv[1])
    profiles = {"buildkit": {}}
    for directory in sorted(root.glob("docker-*")):
        by_phase = phases(load(directory / "trace.jsonl"))
        if {"run1-cold", "run2-warm"} <= by_phase.keys():
            profiles["buildkit"][directory.name.removeprefix("docker-")] = buildkit(directory)
    json.dump(profiles, sys.stdout, indent=1)
    print()


if __name__ == "__main__":
    main()
