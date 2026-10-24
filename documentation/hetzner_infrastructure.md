# Hetzner Production Infrastructure

Operational reference for the live production server that runs the Danish Democracy Data pipeline.
Docker usage on any host is described in [docker/DOCKER_USAGE.md](../docker/DOCKER_USAGE.md).

---

## Server

| Property | Value |
| --- | --- |
| Provider | [Hetzner Cloud](https://console.hetzner.cloud/) |
| Name | *(see Hetzner Cloud console — "Servers")* |
| Type | **CPX42** (8 vCPU, 16 GB RAM) |
| Location | **Nuremberg, DE** (`nbg1`) |
| OS image | **Hetzner "Docker CE" app image** — Docker Engine is pre-installed |
| SSH login | `root@<server-ip>` with key `~/.ssh/id_ed25519` |

> The server IP address is visible in the Hetzner Cloud console under **Servers → Primary IP**.  
> It is intentionally omitted from this document to avoid publishing it.

---

## Attached Block Volumes

Two persistent Hetzner block volumes are attached to the server.  
They are formatted as **ext4** and mounted at boot via `/etc/fstab`.

| Mount point | Size | Purpose |
| --- | --- | --- |
| `/data` | 50 GB | Live operational data (DuckDB, DuckLake, dlt working dir, dbt logs, Dagster home, Metabase state, Bronze files and their archive, local Delta exports, run logs) |
| `/data_backup` | 50 GB | Local backup archives (per-target retention: 62 days dagster/metabase, 7 days duckdb/ducklake) and structured backup logs |

### One-time volume setup (run once after attaching each volume in the Hetzner console)

```bash
# --- /data volume ---
# Find the device name from the Hetzner console (Volumes → click volume → "Mount instructions")
# Hetzner always gives a stable by-id path, e.g.:
#   /dev/disk/by-id/scsi-0HC_Volume_<id>

DEVICE_DATA="/dev/disk/by-id/scsi-0HC_Volume_<live-volume-id>"       # replace with actual ID
DEVICE_BACKUP="/dev/disk/by-id/scsi-0HC_Volume_<backup-volume-id>"   # replace with actual ID

# Format (ONLY the first time — this erases the disk)
mkfs.ext4 -L data        "$DEVICE_DATA"
mkfs.ext4 -L data_backup "$DEVICE_BACKUP"

# Mount points
mkdir -p /data /data_backup

# Mount temporarily to verify
mount "$DEVICE_DATA"   /data
mount "$DEVICE_BACKUP" /data_backup
```

### Persistent mount via `/etc/fstab`

Add these two lines to `/etc/fstab` so the volumes survive reboots.  
Replace the `scsi-0HC_Volume_<id>` tokens with the actual device IDs from the Hetzner console.

```
/dev/disk/by-id/scsi-0HC_Volume_<live-volume-id>    /data        ext4  defaults,nofail,discard  0 2
/dev/disk/by-id/scsi-0HC_Volume_<backup-volume-id>  /data_backup ext4  defaults,nofail,discard  0 2
```

Verify without rebooting:

```bash
mount -a        # re-reads fstab; should produce no output
df -h /data /data_backup
```

### Directory structure inside `/data`

```
/data/
├── dlt_pipelines/          # dlt working directory (owned by UID 1000)
├── duckdb/                 # DuckDB .duckdb file + WAL, DuckLake catalog (owned by UID 1000; only duckdb_server opens them)
├── dbt_logs/               # dbt JSON execution logs (owned by UID 1000)
├── dagster/                # Dagster home — run history, schedules, SQLite (owned by UID 1000)
├── ducklake/               # DuckLake Parquet data, ducklake mode only (owned by UID 1000)
├── local/                  # LOCAL_STORAGE_PATH (owned by UID 1000)
│   ├── Files/
│   │   ├── Bronze/         # extraction files
│   │   ├── Silver/         # Delta export (STORAGE_TARGET=local)
│   │   └── Gold/           # Delta export (STORAGE_TARGET=local)
│   ├── archive/Bronze/     # processed Bronze files (bronze_archive_job), gzip + MANIFEST.ndjson
│   └── logs/               # NDJSON run logs
└── metabase/
    ├── data/               # Metabase application database (owned by UID 2000)
    └── duckdb-extensions/  # DuckDB extensions pre-loaded for Metabase (owned by UID 2000)
```

### Directory structure inside `/data_backup`

```
/data_backup/
├── dagster/    # Timestamped zip archives of /data/dagster
├── metabase/   # Timestamped zip archives of /data/metabase/data
├── duckdb/     # Timestamped zip archives of /data/duckdb (7-day local retention)
├── ducklake/   # Timestamped zip archives of /data/ducklake (7-day local retention; ducklake mode only)
└── logs/       # NDJSON backup run logs (one file per run)
```

### Host permission setup

After mounting the volumes, run the permission setup script **once** to create
all sub-directories and apply the correct ownership:

```bash
sudo scripts/setup_host_permissions.sh
```

What this script does:

1. Creates every sub-directory listed above.
2. Sets `app` (UID 1000) as owner of pipeline and backup directories.
3. Sets Metabase user (UID 2000) as owner of `/data/metabase/`.
4. Applies `o+rX` to `/data/metabase/data` so the backup container (UID 1000) can read Metabase state.
   Metabase needs no access to `/data/duckdb/` or `/data/ducklake/`: it connects through `duckdb_server`.
5. Detects the Docker socket GID and writes `DOCKER_GID=<gid>` into `.env`.

Dry-run mode (prints intended actions, makes no changes):

```bash
sudo scripts/setup_host_permissions.sh --dry-run
```

---

## SSH Keys

### Operator access to the server

The server is accessed from operator laptops using an **Ed25519 SSH key**.

| Item | Value |
| --- | --- |
| Key algorithm | Ed25519 |
| Default private key path (on operator laptop) | `~/.ssh/id_ed25519` |
| Default public key path (on operator laptop) | `~/.ssh/id_ed25519.pub` |
| Authorized location on server | `/root/.ssh/authorized_keys` |

The deploy script ([scripts/deploy.sh](../scripts/deploy.sh)) reads the operator key via
the `DEPLOY_KEY` environment variable, defaulting to `~/.ssh/id_ed25519`.

### GitHub deploy key (server → GitHub, unattended)

The server needs read access to the GitHub repository so that `deploy.sh` can run
`git fetch origin main` and `git reset --hard origin/main` unattended (no password prompt).

This is implemented via a **GitHub Deploy Key** — a separate Ed25519 key pair where:

- The **private key** lives on the server at `/root/.ssh/id_ed25519`
  (or another path configured in the server's `~/.ssh/config`).
- The **public key** is registered in GitHub under:
  **Repository → Settings → Deploy keys** with **read-only** access.

To generate and register a deploy key:

```bash
# On the server — generate the key pair (no passphrase for unattended use)
ssh-keygen -t ed25519 -C "ddd-deploy@hetzner-prod" -f /root/.ssh/id_ed25519 -N ""

# Print the public key — copy this into GitHub
cat /root/.ssh/id_ed25519.pub

# Verify GitHub is reachable
ssh -T git@github.com
# Expected output: "Hi edwinweber/dbt_duckdb_demo! You've successfully authenticated..."
```

Register the public key at:
`https://github.com/edwinweber/dbt_duckdb_demo/settings/keys`
→ **Add deploy key** → Title: `hetzner-prod` → Paste public key → **Allow write access: NO**

### StorageBox SSH key (server → Hetzner StorageBox)

The backup service uploads archives to a Hetzner StorageBox via `rsync` over SSH port 23.
The host's SSH directory is mounted read-only into the `backup` container at `/home/app/.ssh`.

| Item | Value |
| --- | --- |
| Host SSH directory | `/data_backup/.ssh` (configurable via `HOST_SSH_DIR` in `.env`) |
| Mounted at (inside container) | `/home/app/.ssh` (read-only) |
| SSH key for StorageBox | `/home/app/.ssh/hetzner` (set via `HETZNER_STORAGEBOX_SSH_KEY`) |
| StorageBox SSH port | **23** (Hetzner always uses 23 for StorageBox) |

#### Staging the key for the non-root container user

The `backup` container runs as `app` (UID 1000), **not** root. The StorageBox
private key therefore cannot live in `/root/.ssh`: that directory is mode `700`
owned by root, and the key itself is mode `600` owned by root, so UID 1000 can
neither traverse the directory nor read the key when it is bind-mounted into the
container. SSH then reports the key as missing.

Copy the key into a directory that UID 1000 owns and can read, and point
`HOST_SSH_DIR` at it:

```bash
sudo mkdir -p /data_backup/.ssh
sudo cp /root/.ssh/hetzner /data_backup/.ssh/hetzner
sudo chown -R 1000:1000 /data_backup/.ssh
sudo chmod 700 /data_backup/.ssh
sudo chmod 600 /data_backup/.ssh/hetzner
```

The `600` mode on the key (owned by `1000:1000`, not loosened to `644`) is
required — SSH refuses to use a private key that is group- or world-readable.
With this in place, `.env` sets `HOST_SSH_DIR=/data_backup/.ssh`, which is
mounted at `/home/app/.ssh`, so the key resolves to `/home/app/.ssh/hetzner`
inside the container.

To authorize the server key on the StorageBox:

```bash
# From the server — add the public key to the StorageBox authorized_keys
ssh-copy-id -p 23 -i /root/.ssh/hetzner.pub \
    <storagebox-user>@<storagebox-host>

# Verify
ssh -p 23 <storagebox-user>@<storagebox-host>
```

StorageBox connection variables in `.env`:

```bash
HETZNER_STORAGEBOX_HOST="u<number>.your-storagebox.de"
HETZNER_STORAGEBOX_USER="u<number>"
HETZNER_STORAGEBOX_PORT=23
HETZNER_STORAGEBOX_REMOTE_DIR="backups/ddd"
HOST_SSH_DIR="/data_backup/.ssh"
HETZNER_STORAGEBOX_SSH_KEY="/home/app/.ssh/hetzner"
```

---

## Firewall

Firewall name: *(see Hetzner Cloud console → **Firewalls**)*

Inbound access is restricted to **two whitelisted IP addresses** — the operator's home IP
and NordVPN exit IP. All other inbound traffic is blocked.

> Actual IP addresses are not stored in this repository. They are visible in the Hetzner
> Cloud console under **Firewalls → <firewall name> → Inbound rules**.

### Open inbound ports

| Port | Protocol | Service |
| --- | --- | --- |
| 22 | TCP | SSH (operator access, deploy script) |
| 3000 | TCP | Dagster UI |
| 3001 | TCP | Metabase BI |

All other inbound ports are blocked. That includes 9494: Docker publishes the
quack server's port on the host, but the firewall only lets it through when you
add a rule for it (remote quack clients, see
[DOCKER_USAGE.md](../docker/DOCKER_USAGE.md#remote-access-to-the-duckdb-server)). There is no HTTPS/TLS termination — access
to Dagster and Metabase relies entirely on the IP whitelist.

### Adding or changing whitelisted IPs

Update the firewall rules in the Hetzner Cloud console:
**Firewalls → <firewall name> → Edit → Inbound rules → Source IP** for each port.

---

## Docker Compose Services

The application stack is defined in [docker-compose.yml](../docker-compose.yml).
Five services are configured; `duckdb_server`, `dagster` and `metabase` run persistently.

| Service | Container name | Host port | Memory limit | Restart policy | Purpose |
|---|---|---|---|---|---|
| `duckdb_server` | `ddd-duckdb-server` | 9494 (`QUACK_SERVER_BIND`) | 8 GB | `unless-stopped` | The only DuckDB writer; quack server for all other containers |
| `dagster` | `ddd-dagster` | 3000 | 6 GB | `unless-stopped` | Dagster webserver + daemon |
| `metabase` | `ddd-metabase` | 3001 | 4 GB | `unless-stopped` | Metabase BI, a quack client of `duckdb_server` |
| `run` | `ddd-run` | — | — | none | One-off Python module runner |
| `backup` | `ddd-backup` | — | — | none | Backup and restore runner |

`dagster` and `metabase` have HTTP health checks (30 s interval); `duckdb_server`
has a TCP check on port 9494 (15 s interval); `run`, `dagster` and `metabase` wait
until it is healthy.

### Docker socket access

`backup` mounts `/var/run/docker.sock` so it can stop and start the container of
the target it backs up (`ddd-duckdb-server` for `duckdb` and `ducklake`,
`ddd-metabase` and `ddd-dagster` for their own data). Nothing is stopped for dbt
runs: Metabase and Dagster are quack clients of `duckdb_server`. The socket GID
must be set in `.env`:

```bash
echo "DOCKER_GID=$(stat -c '%g' /var/run/docker.sock)" >> .env
```

---

## Deploying a New Version

Run from an **operator laptop** (not the server):

```bash
# Optionally configure via .env.deploy (copy from .env.deploy.example)
cp .env.deploy.example .env.deploy
# Edit DEPLOY_HOST, DEPLOY_USER, DEPLOY_PATH

./scripts/deploy.sh
```

The script:

1. SSHs into the server using `~/.ssh/id_ed25519` (or `DEPLOY_KEY`).
2. Runs `git fetch origin main && git reset --hard origin/main` on the server.
3. Runs `docker compose down --remove-orphans`.
4. Runs `docker compose build`.
5. Runs `docker compose up -d dagster metabase`, which starts `duckdb_server`
   first (both depend on it being healthy).

A deploy does not run anything else. When a release needs a one-off step (for
example renaming old full extractions with `mark_full_extracts`), run it after the
deploy and before the next Silver run, and take a backup first
(`docker compose run --rm backup python -m ddd_python.ddd_utils.backup_platform --targets ducklake duckdb`).

The repository on the server must have the GitHub deploy key authorised so that
`git fetch` succeeds without a password prompt.

---

## Backup and Nightly Cron

### Manual backup

```bash
# Back up all targets (Dagster, Metabase, DuckDB, and DuckLake in ducklake mode)
docker compose run --rm backup

# Back up a single target
docker compose run --rm backup python -m ddd_python.ddd_utils.backup_platform --targets dagster
```

### Nightly cron (runs at 02:00 server time)

Install the cron entry with:

```bash
scripts/setup_backup_cron.sh --install
```

The entry written to crontab:

```
0 2 * * * DOCKER_HOST=unix:///var/run/docker.sock cd "/opt/dbt_duckdb_demo" && docker compose run --rm backup >> /data_backup/logs/cron.log 2>&1
```

Cron output goes to `/data_backup/logs/cron.log`.
Structured NDJSON run logs go to `/data_backup/logs/backup_log_<timestamp>.ndjson`.

The targets cover the databases (`duckdb`, `ducklake`), Dagster and Metabase. The
Bronze files and the Bronze archive under `/data/local` are not a backup target.
Since Silver is the state, the archive is only needed for a replay, but it is then
the only copy of the original extractions; copy it off the server (for example
to the StorageBox) if you want to keep that possibility.

### Backup retention

| Location | Retention |
| --- | --- |
| `/data_backup/dagster/` | 62 days |
| `/data_backup/metabase/` | 62 days |
| `/data_backup/duckdb/` | 7 days |
| `/data_backup/ducklake/` | 7 days (ducklake mode only) |
| Hetzner StorageBox | 31 daily + 12 monthly archives per target (grandfather-father-son rotation) |

The StorageBox rotation keeps the most recent archive of each of the last 31 days
plus the most recent archive of each of the last 12 months, **per target**. Older
archives are deleted after each successful upload. Multiple runs on the same day
collapse to the latest run. Tune via `HETZNER_STORAGEBOX_KEEP_DAILY` /
`HETZNER_STORAGEBOX_KEEP_MONTHLY` in `.env` (set both to `0` to keep everything
remotely, the previous behaviour).

---

## Quick Reference: Day-1 Setup Checklist

Use this checklist when provisioning a new server from scratch.

```
[ ] Create CPX42 server in Hetzner Cloud console (nbg1, Docker CE image)
[ ] Note the server IP — add it to your SSH config if desired
[ ] Attach two block volumes (50 GB each) in the Hetzner console
[ ] Format and fstab-mount the volumes as /data and /data_backup (see above)
[ ] Clone the repo: git clone git@github.com:edwinweber/dbt_duckdb_demo.git /opt/dbt_duckdb_demo
[ ] Generate deploy key on server and register public key in GitHub (read-only)
[ ] Copy .env.example → .env and fill in all values, including QUACK_SERVER_TOKEN
[ ] Copy .env.deploy.example → .env.deploy on your laptop and fill in values
[ ] sudo scripts/setup_host_permissions.sh   (creates dirs, sets ownership, writes DOCKER_GID)
[ ] docker compose build
[ ] docker compose up -d dagster metabase
[ ] Verify health: curl http://localhost:3000  and  curl http://localhost:3001/api/health
[ ] Metabase: add the DuckDB database once (path :memory:, init SQL from /data/metabase/data/duckdb-init.sql)
[ ] Configure Hetzner Firewall — whitelist operator IPs for ports 22, 3000, 3001
[ ] Attach firewall to the server in Hetzner console
[ ] Stage StorageBox SSH key for UID 1000 in /data_backup/.ssh (see "Staging the key" above)
[ ] Set up StorageBox SSH key and verify rsync: docker compose run --rm backup
[ ] scripts/setup_backup_cron.sh --install   (nightly 02:00 backup)
[ ] First load: full extractions of the incremental entities (date_to_load_from 1900-01-01), then Silver
[ ] Verify end-to-end: in the Dagster UI run full_pipeline_job, then enable the schedules
```
