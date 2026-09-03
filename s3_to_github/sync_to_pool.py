#!/usr/bin/env python3
import ast
import os
import re
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Optional
from urllib.parse import quote, urlparse

import boto3
import requests
from botocore.config import Config

TIMEOUT = 30
USER_AGENT = "pic-hosting-cluster-storage-pool/1.0"


@dataclass
class Node:
    provider: str
    name: str
    token: str
    clone_url: str
    used_bytes: Optional[int]
    file_count: int
    paths: set[str] = field(default_factory=set)


@dataclass
class Source:
    provider: str
    name: str
    bucket: str
    client: object
    prefix: str


@dataclass
class SourceCopy:
    source: Source
    key: str
    size: int
    etag: str


@dataclass
class LogicalObject:
    key: str
    size: int
    copies: list[SourceCopy]


STRING_LITERAL = r'''(?:"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*')'''


def die(message: str) -> None:
    print(f"::error::{message}")
    raise SystemExit(1)


def warn(message: str) -> None:
    print(f"::warning::{message}")


def decode_js_string(value: str) -> str:
    try:
        return ast.literal_eval(value)
    except Exception:
        return value[1:-1]


def extract_const(source: str, name: str, default=None):
    pattern = re.compile(
        rf"\b(?:const|let|var)\s+{re.escape(name)}\s*=\s*({STRING_LITERAL}|true|false|[^;\n]+)",
        re.I,
    )
    match = pattern.search(source)
    if not match:
        return default
    raw = match.group(1).split("//", 1)[0].strip()
    raw = re.sub(r"/\*.*?\*/", "", raw, flags=re.S).strip()
    while raw.startswith("(") and raw.endswith(")"):
        raw = raw[1:-1].strip()
    string_match = re.search(STRING_LITERAL, raw)
    if string_match:
        return decode_js_string(string_match.group(0))
    if raw.lower() == "true":
        return True
    if raw.lower() == "false":
        return False
    return raw


def extract_array_text(source: str, name: str) -> Optional[str]:
    match = re.search(rf"\b(?:const|let|var)\s+{re.escape(name)}\s*=\s*\[", source)
    if not match:
        return None

    start = match.end() - 1
    depth = 0
    quote_char = None
    escaped = False
    i = start
    while i < len(source):
        ch = source[i]
        nxt = source[i + 1] if i + 1 < len(source) else ""

        if quote_char:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == quote_char:
                quote_char = None
            i += 1
            continue

        if ch in ("'", '"', '`'):
            quote_char = ch
            i += 1
            continue

        if ch == "/" and nxt == "/":
            nl = source.find("\n", i + 2)
            i = len(source) if nl == -1 else nl + 1
            continue

        if ch == "/" and nxt == "*":
            end = source.find("*/", i + 2)
            i = len(source) if end == -1 else end + 2
            continue

        if ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0:
                return source[start : i + 1]
        i += 1

    return None


def extract_array_objects(source: str, name: str) -> list[dict[str, str]]:
    array_text = extract_array_text(source, name)
    if not array_text:
        return []

    objects = []
    for body in re.findall(r"\{(.*?)\}", array_text, flags=re.S):
        obj = {}
        field_pattern = re.compile(rf"([A-Za-z_$][\w$]*)\s*:\s*({STRING_LITERAL})")
        for key, value in field_pattern.findall(body):
            obj[key] = decode_js_string(value)
        if obj:
            objects.append(obj)
    return objects


def get_worker_source() -> str:
    account_id = os.environ.get("ACCOUNT_ID", "").strip()
    worker_name = os.environ.get("WORKER_NAME", "").strip()
    api_token = os.environ.get("API_TOKEN", "").strip()
    if not all((account_id, worker_name, api_token)):
        die("ACCOUNT_ID, WORKER_NAME and API_TOKEN are required")

    url = (
        f"https://api.cloudflare.com/client/v4/accounts/{quote(account_id, safe='')}"
        f"/workers/scripts/{quote(worker_name, safe='')}/content"
    )
    response = requests.get(
        url,
        headers={"Authorization": f"Bearer {api_token}", "User-Agent": USER_AGENT},
        timeout=TIMEOUT,
    )
    if not response.ok:
        die(f"failed to read Worker source: HTTP {response.status_code}: {response.text[:300]}")
    return response.text


def build_github_nodes(worker: str, gitlab_configs: list[dict[str, str]]) -> list[dict[str, str]]:
    explicit = extract_array_objects(worker, "GITHUB_CONFIGS")
    username = str(extract_const(worker, "GITHUB_USERNAME", "") or "").strip()
    global_pat = str(extract_const(worker, "GITHUB_PAT", "") or "").strip()

    if explicit:
        configs = []
        for cfg in explicit:
            repo = cfg.get("repo") or cfg.get("name") or ""
            owner = cfg.get("owner") or cfg.get("username") or username
            token = cfg.get("token") or cfg.get("pat") or global_pat
            name = cfg.get("name") or repo
            if owner and repo and token:
                configs.append({"name": name, "owner": owner, "repo": repo, "token": token})
        return configs

    # Backward compatibility with the original Worker: GitHub repository names
    # are derived from GITLAB_CONFIGS[].name.
    if not username or not global_pat:
        return []
    return [
        {"name": cfg["name"], "owner": username, "repo": cfg["name"], "token": global_pat}
        for cfg in gitlab_configs
        if cfg.get("name")
    ]


def github_node(cfg: dict[str, str], directory: str) -> Optional[Node]:
    owner, repo, token = cfg["owner"], cfg["repo"], cfg["token"]
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": USER_AGENT,
    }
    response = requests.get(
        f"https://api.github.com/repos/{quote(owner, safe='')}/{quote(repo, safe='')}",
        headers=headers,
        timeout=TIMEOUT,
    )
    if not response.ok:
        warn(f"GitHub node {owner}/{repo} unavailable: HTTP {response.status_code}")
        return None
    meta = response.json()
    branch = meta.get("default_branch") or "main"
    used_bytes = int(meta.get("size") or 0) * 1024
    paths = set()

    tree = requests.get(
        f"https://api.github.com/repos/{quote(owner, safe='')}/{quote(repo, safe='')}/git/trees/{quote(branch, safe='')}?recursive=1",
        headers=headers,
        timeout=TIMEOUT,
    )
    if tree.status_code not in (404, 409):
        if not tree.ok:
            warn(f"cannot read GitHub tree for {owner}/{repo}: HTTP {tree.status_code}")
        else:
            payload = tree.json()
            if payload.get("truncated"):
                warn(f"GitHub tree for {owner}/{repo} is truncated; quantity/existence data may be incomplete")
            prefix = f"{directory.rstrip('/')}/" if directory else ""
            for item in payload.get("tree", []):
                path = item.get("path", "")
                if item.get("type") == "blob" and (not prefix or path.startswith(prefix)):
                    paths.add(path)

    return Node(
        provider="github",
        name=cfg.get("name") or repo,
        token=token,
        clone_url=f"https://github.com/{owner}/{repo}.git",
        used_bytes=used_bytes,
        file_count=len(paths),
        paths=paths,
    )


def gitlab_node(cfg: dict[str, str], directory: str) -> Optional[Node]:
    project_id, token = cfg.get("id", ""), cfg.get("token", "")
    if not project_id or not token:
        return None
    headers = {"PRIVATE-TOKEN": token, "User-Agent": USER_AGENT}
    response = requests.get(
        f"https://gitlab.com/api/v4/projects/{quote(str(project_id), safe='')}",
        params={"statistics": "true"},
        headers=headers,
        timeout=TIMEOUT,
    )
    if not response.ok:
        warn(f"GitLab node {cfg.get('name') or project_id} unavailable: HTTP {response.status_code}")
        return None
    meta = response.json()
    project_path = meta.get("path_with_namespace")
    if not project_path:
        warn(f"GitLab project {project_id} has no path_with_namespace")
        return None

    statistics = meta.get("statistics") or {}
    used_bytes = statistics.get("repository_size")
    if used_bytes is None:
        used_bytes = statistics.get("storage_size")
    used_bytes = int(used_bytes) if used_bytes is not None else None

    branch = meta.get("default_branch") or "main"
    paths = set()
    page = 1
    prefix_path = directory.strip("/")
    while True:
        params = {"recursive": "true", "per_page": 100, "page": page, "ref": branch}
        if prefix_path:
            params["path"] = prefix_path
        tree = requests.get(
            f"https://gitlab.com/api/v4/projects/{quote(str(project_id), safe='')}/repository/tree",
            params=params,
            headers=headers,
            timeout=TIMEOUT,
        )
        if tree.status_code in (404, 409):
            break
        if not tree.ok:
            warn(f"cannot read GitLab tree for {project_path}: HTTP {tree.status_code}")
            break
        for item in tree.json():
            if item.get("type") == "blob" and item.get("path"):
                paths.add(item["path"])
        next_page = tree.headers.get("X-Next-Page")
        if not next_page:
            break
        page = int(next_page)

    return Node(
        provider="gitlab",
        name=cfg.get("name") or meta.get("path") or str(project_id),
        token=token,
        clone_url=f"https://gitlab.com/{project_path}.git",
        used_bytes=used_bytes,
        file_count=len(paths),
        paths=paths,
    )


def endpoint_host(value: str) -> str:
    value = value.strip().rstrip("/")
    parsed = urlparse(value if "://" in value else f"https://{value}")
    return parsed.netloc or parsed.path


def build_sources(worker: str, directory: str) -> list[Source]:
    result = []
    retry_config = Config(signature_version="s3v4", retries={"max_attempts": 4, "mode": "standard"})

    for cfg in extract_array_objects(worker, "R2_CONFIGS"):
        required = (cfg.get("accountId"), cfg.get("accessKeyId"), cfg.get("secretAccessKey"), cfg.get("bucket"))
        if not all(required):
            continue
        client = boto3.client(
            "s3",
            endpoint_url=f"https://{cfg['accountId']}.r2.cloudflarestorage.com",
            aws_access_key_id=cfg["accessKeyId"],
            aws_secret_access_key=cfg["secretAccessKey"],
            region_name="auto",
            config=retry_config,
        )
        prefix_dir = (cfg.get("dir") or directory).strip("/")
        result.append(Source("r2", cfg.get("name") or cfg["accountId"], cfg["bucket"], client, f"{prefix_dir}/" if prefix_dir else ""))

    for cfg in extract_array_objects(worker, "B2_CONFIGS"):
        required = (cfg.get("endPoint"), cfg.get("keyId"), cfg.get("applicationKey"), cfg.get("bucket"))
        if not all(required):
            continue
        host = endpoint_host(cfg["endPoint"])
        parts = host.split(".")
        region = parts[1] if len(parts) > 2 and parts[0] == "s3" else "us-east-1"
        client = boto3.client(
            "s3",
            endpoint_url=f"https://{host}",
            aws_access_key_id=cfg["keyId"],
            aws_secret_access_key=cfg["applicationKey"],
            region_name=region,
            config=retry_config,
        )
        prefix_dir = (cfg.get("dir") or directory).strip("/")
        result.append(Source("b2", cfg.get("name") or host, cfg["bucket"], client, f"{prefix_dir}/" if prefix_dir else ""))
    return result


def list_source_copies(sources: list[Source]) -> list[SourceCopy]:
    copies = []
    for source in sources:
        print(f"Scanning {source.provider}:{source.name}/{source.bucket} prefix={source.prefix or '/'}")
        paginator = source.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=source.bucket, Prefix=source.prefix):
            for item in page.get("Contents", []):
                key = item.get("Key", "")
                if not key or key.endswith("/"):
                    continue
                copies.append(SourceCopy(source, key, int(item.get("Size") or 0), str(item.get("ETag") or "").strip('"')))
    return copies


def coalesce_copies(copies: list[SourceCopy]) -> list[LogicalObject]:
    by_key: dict[str, LogicalObject] = {}
    for copy in copies:
        current = by_key.get(copy.key)
        if current is None:
            by_key[copy.key] = LogicalObject(copy.key, copy.size, [copy])
            continue
        first = current.copies[0]
        if copy.size == first.size and copy.etag and first.etag and copy.etag == first.etag:
            current.copies.append(copy)
        elif copy.size == first.size and (not copy.etag or not first.etag):
            current.copies.append(copy)
        else:
            warn(f"conflicting staging copies for {copy.key}; leaving the conflicting copy untouched")
    return list(by_key.values())


def choose_node(nodes: list[Node], strategy: str) -> Node:
    strategy = (strategy or "size").strip()
    lower = strategy.lower()
    if lower == "size":
        if all(node.used_bytes is not None for node in nodes):
            return min(nodes, key=lambda n: (n.used_bytes, n.file_count, n.provider, n.name))
        warn("at least one provider does not expose repository size; falling back to quantity strategy")
        return min(nodes, key=lambda n: (n.file_count, n.provider, n.name))
    if lower == "quantity":
        return min(nodes, key=lambda n: (n.file_count, n.used_bytes or 0, n.provider, n.name))

    if ":" in strategy:
        provider, name = strategy.split(":", 1)
        matches = [n for n in nodes if n.provider.lower() == provider.lower() and n.name == name]
    else:
        matches = [n for n in nodes if n.provider == "github" and n.name == strategy]
        if not matches:
            matches = [n for n in nodes if n.name == strategy]
    if not matches:
        raise RuntimeError(f"STRATEGY={strategy!r} does not match a writable node")
    return matches[0]


def safe_relative_path(key: str) -> PurePosixPath:
    path = PurePosixPath(key.lstrip("/"))
    if not path.parts or any(part in ("", ".", "..") for part in path.parts):
        raise ValueError(f"unsafe object key: {key!r}")
    return path


def git_credentials(node: Node, path: Path) -> None:
    if node.provider == "github":
        username, host = "x-access-token", "github.com"
    elif node.provider == "gitlab":
        username, host = "oauth2", "gitlab.com"
    else:
        raise ValueError(node.provider)
    path.write_text(f"https://{quote(username, safe='')}:{quote(node.token, safe='')}@{host}\n", encoding="utf-8")
    path.chmod(0o600)


def run_git(args: list[str], cwd: Optional[Path], cred_file: Path, env: dict[str, str]) -> None:
    subprocess.run(["git", "-c", f"credential.helper=store --file={cred_file}", *args], cwd=cwd, env=env, check=True)


def persist_group(node: Node, objects: list[LogicalObject]) -> list[LogicalObject]:
    if not objects:
        return []
    print(f"Persisting {len(objects)} object(s) to {node.provider}:{node.name}")
    env = dict(os.environ)
    env["GIT_TERMINAL_PROMPT"] = "0"

    with tempfile.TemporaryDirectory(prefix="storage-pool-") as tmp:
        tmp_path = Path(tmp)
        repo_dir = tmp_path / "repo"
        cred_file = tmp_path / "git-credentials"
        git_credentials(node, cred_file)
        try:
            run_git(["clone", "--depth", "1", node.clone_url, str(repo_dir)], None, cred_file, env)
            run_git(["checkout", "-B", "main"], repo_dir, cred_file, env)
            run_git(["config", "user.name", "pic-hosting-cluster"], repo_dir, cred_file, env)
            run_git(["config", "user.email", "actions@users.noreply.github.com"], repo_dir, cred_file, env)

            for obj in objects:
                rel = safe_relative_path(obj.key)
                destination = repo_dir.joinpath(*rel.parts)
                destination.parent.mkdir(parents=True, exist_ok=True)
                primary = obj.copies[0]
                primary.source.client.download_file(primary.source.bucket, primary.key, str(destination))

            run_git(["add", "--all"], repo_dir, cred_file, env)
            changed = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=repo_dir, env=env).returncode != 0
            if changed:
                run_git(["commit", "-m", f"Store {len(objects)} file(s) via storage pool"], repo_dir, cred_file, env)
                run_git(["push", "origin", "HEAD:main"], repo_dir, cred_file, env)
            else:
                print(f"No content changes for {node.provider}:{node.name}; treating objects as already persisted")
            return objects
        except Exception as exc:
            warn(f"failed to persist batch to {node.provider}:{node.name}: {exc}")
            return []


def delete_copies(objects: list[LogicalObject]) -> int:
    deleted = 0
    for obj in objects:
        for copy in obj.copies:
            try:
                copy.source.client.delete_object(Bucket=copy.source.bucket, Key=copy.key)
                deleted += 1
            except Exception as exc:
                warn(f"persisted {copy.key} but failed to delete staging copy from {copy.source.provider}:{copy.source.name}: {exc}")
    return deleted


def main() -> None:
    worker = get_worker_source()
    directory = str(extract_const(worker, "DIR", "") or "").strip("/")
    strategy = str(extract_const(worker, "STRATEGY", "size") or "size").strip()
    delete_value = extract_const(worker, "DELETE", "true")
    delete_after = str(delete_value).strip().lower() not in {"false", "0", "no", "off", "none", ""}

    gitlab_configs = [cfg for cfg in extract_array_objects(worker, "GITLAB_CONFIGS") if cfg.get("id") and cfg.get("token") and cfg.get("name")]
    github_configs = build_github_nodes(worker, gitlab_configs)

    nodes: list[Node] = []
    for cfg in github_configs:
        node = github_node(cfg, directory)
        if node:
            nodes.append(node)
    for cfg in gitlab_configs:
        node = gitlab_node(cfg, directory)
        if node:
            nodes.append(node)
    if not nodes:
        die("no writable GitHub/GitLab nodes were discovered from the Worker")

    print("Destination pool:")
    for node in nodes:
        size_text = "unknown" if node.used_bytes is None else f"{node.used_bytes / 1024 / 1024:.1f} MiB"
        print(f"  - {node.provider}:{node.name}: files={node.file_count}, size={size_text}")

    sources = build_sources(worker, directory)
    if not sources:
        die("no R2/B2 S3-compatible staging sources were discovered from the Worker")

    logical_objects = coalesce_copies(list_source_copies(sources))
    if not logical_objects:
        print("No staging objects found; nothing to do")
        return

    path_owners: dict[str, list[Node]] = {}
    for node in nodes:
        for path in node.paths:
            path_owners.setdefault(path, []).append(node)

    already_persisted = []
    pending = []
    for obj in logical_objects:
        if obj.key in path_owners:
            owners = ", ".join(f"{n.provider}:{n.name}" for n in path_owners[obj.key])
            print(f"Already persisted: {obj.key} -> {owners}")
            already_persisted.append(obj)
        else:
            pending.append(obj)

    groups: dict[str, list[LogicalObject]] = {}
    node_index = {f"{n.provider}:{n.name}": n for n in nodes}
    for obj in sorted(pending, key=lambda x: x.key):
        node = choose_node(nodes, strategy)
        key = f"{node.provider}:{node.name}"
        groups.setdefault(key, []).append(obj)
        node.file_count += 1
        if node.used_bytes is not None:
            node.used_bytes += obj.size
        node.paths.add(obj.key)

    persisted = []
    for key, objects in groups.items():
        persisted.extend(persist_group(node_index[key], objects))

    deletable = [*already_persisted, *persisted]
    deleted = delete_copies(deletable) if delete_after else 0
    print(
        "Summary: "
        f"staging_objects={len(logical_objects)}, "
        f"already_persisted={len(already_persisted)}, "
        f"newly_persisted={len(persisted)}, "
        f"staging_copies_deleted={deleted}, "
        f"delete={delete_after}, strategy={strategy}"
    )

    failed = len(pending) - len(persisted)
    if failed:
        die(f"{failed} object(s) could not be persisted; staging copies were kept")


if __name__ == "__main__":
    main()
