# AGENTS.md — retaildata

## Mission

`retaildata` is a **unified, streamlined dataset fetcher** for realistic retail datasets (forecasting, promo/price, inventory, baskets, causal impact, customer journey).

It provides:
- A **CLI** (`uvx retaildata ...`) for one-command downloads into a user-chosen data directory.
- A **Python API** (`import retaildata`) for use in notebooks / pipelines.
- **Secure credential management** for sources that require auth (Kaggle, gated portals, etc.).
- **Cache/database management** with easy “clear some/all” operations.

Core philosophy:
- **“Don’t make the user think.”** Most commands should work without reading docs.
- **Security > convenience**, but still pleasant UX.
- Prefer **well-audited crypto + OS keychain** over custom crypto.

---

## Non-goals

- Not a modeling library. No forecasting models inside.
- Not a generic downloader for everything on the internet.
- No scraping behind logins unless explicitly allowed and legally safe.

---

## Primary user stories

1) **One-time credentials setup**
- User enters Kaggle API token once.
- Token is stored securely.
- Future downloads work without re-entering.

2) **Download a dataset**
- `uvx retaildata get m5 -o ./data`
- Data lands in a predictable structure, with metadata and checksums.

3) **Manage local storage**
- List what’s downloaded, where it lives, and size:
  - `uvx retaildata ls`
- Clear cache for one dataset or all:
  - `uvx retaildata rm m5`
  - `uvx retaildata purge --all`
- Turn caching on/off; control cache/data locations.

4) **Use inside Python**
- `retaildata.load("m5", data_dir="...")`
- Returns clean file paths and/or ready-to-use DataFrames (optional).

---

## Architecture overview

### Package modules
- `retaildata/cli.py`
  - Typer-based CLI entry point.
- `retaildata/config.py`
  - Config model (paths, cache mode, providers enabled).
- `retaildata/credentials/`
  - `store.py`: unified interface for credential storage & retrieval.
  - `keyring_store.py`: OS keychain backend (preferred).
  - `encrypted_file_store.py`: encrypted file backend (fallback).
- `retaildata/providers/`
  - One provider per source:
    - `kaggle.py`
    - `hf.py` (Hugging Face datasets)
    - `uci.py`
    - `openml.py`
    - `http.py` (simple public URLs)
- `retaildata/cache/`
  - `manager.py`: cache index, hashing, pruning, rm/purge
  - optionally `sqlite_index.py` for fast inventory of files
- `retaildata/datasets/registry.py`
  - Dataset registry: identifiers -> provider + parameters + postprocessing
- `retaildata/postprocess/`
  - Normalization (folder layout, metadata.json, checksums)
- `retaildata/api.py`
  - Python API: `get()`, `load()`, `list_datasets()`, `purge()`

### Data layout (default)
Inside `data_dir`:
- `raw/<dataset_id>/...` (downloaded archives / original files)
- `prepared/<dataset_id>/...` (optional normalized layout)
- `meta/<dataset_id>/metadata.json`
- `meta/<dataset_id>/checksums.json`

If caching enabled and `data_dir` is not set:
- Use `platformdirs` default:
  - cache: `~/.cache/retaildata/`
  - data:  `~/.local/share/retaildata/` (or OS equivalent)

---

## Security model for credentials (MUST FOLLOW)

### Preferred: OS keychain via `keyring`
- Store secrets in:
  - macOS Keychain / Windows Credential Manager / Secret Service (Linux)
- This is the “cryptographically best” *practical* solution because:
  - encryption-at-rest + OS hardening
  - no custom key management
  - integrates with system lock/login

Implementation rules:
- Service name: `retaildata`
- Secret keys:
  - `kaggle:username`
  - `kaggle:key`
  - etc.

### Fallback: encrypted file store (AES-256-GCM + Argon2id)
If keyring is unavailable (headless servers, minimal Linux), use an encrypted file store:

- Encryption: **AES-256-GCM** (AEAD)
- Key derivation: **Argon2id** from a user passphrase (via `argon2-cffi`)
- Store on disk: `~/.config/retaildata/credentials.enc` (or under data dir if requested)
- File format must include:
  - version
  - kdf params (time_cost, memory_cost, parallelism)
  - salt
  - nonce
  - ciphertext
  - auth tag (from GCM)

UX:
- On first `retaildata auth set kaggle`, prompt:
  - “Use system keychain? (recommended)” if keyring works
  - else ask for passphrase (with confirmation)
- Support non-interactive CI via env vars (never write to disk unless user explicitly asks).

### Rules / threat assumptions
- If an attacker fully compromises the running machine/user account, secrets can be extracted.
- Goal is strong **encryption-at-rest** and safe default handling to protect against:
  - someone finding files on disk
  - accidental repo commits
  - casual snooping

Never:
- Print secrets in logs.
- Store secrets in plaintext config.
- Store secrets alongside dataset data by default.

---

## Cache & storage management

### Modes
- `cache = "on"` (default): reuse downloaded artifacts
- `cache = "off"`: always redownload; still write into `data_dir` if requested
- `cache = "db"` (optional): store index in SQLite for speed, data still on filesystem

### Management commands (CLI)
- `retaildata ls` — list downloaded datasets, versions, size, locations
- `retaildata info <dataset>` — show provider, required creds, files, checksums
- `retaildata rm <dataset>` — remove dataset files + metadata
- `retaildata purge --all` — remove everything
- `retaildata cache set on|off`
- `retaildata cache dir <path>`
- `retaildata data dir <path>`

Must be safe:
- Require confirmation for `purge --all` unless `--yes`.

---

## CLI design (must be uvx-friendly)

Entry point: `retaildata`

Examples:
- `uvx retaildata list`
- `uvx retaildata get m5 -o ./data`
- `uvx retaildata get favorita --prepare -o ./data`
- `uvx retaildata auth set kaggle` (interactive)
- `uvx retaildata auth status`
- `uvx retaildata auth rm kaggle`
- `uvx retaildata rm m5`
- `uvx retaildata purge --all --yes`

CLI principles:
- sensible defaults
- minimal required flags
- clear error messages with next steps
- `--json` output option for automation

---

## Python API

Minimal stable surface:
- `retaildata.list_datasets() -> list[DatasetInfo]`
- `retaildata.get(dataset_id, data_dir=None, cache=True, prepare=False, **opts) -> DatasetPaths`
- `retaildata.load(dataset_id, ...) -> object` (optional convenience: dataframe dict)
- `retaildata.purge(dataset_id=None, all=False)`

Return types must be well-typed dataclasses.

---

## Dataset registry requirements

Each dataset must define:
- `id` (short, stable): `m5`, `favorita`, `rossmann`, `instacart`, `online_retail_uci`, …
- `topic_tags`: `forecasting`, `promo`, `pricing`, `inventory`, `baskets`, `causal`, `crm`
- `provider`: `kaggle|hf|uci|openml|http`
- `requires_credentials`: bool
- `license_notes` and any access constraints
- `default_output_layout` and optional `prepare()` step

---

## Quality standards

### Tests
- Unit tests for:
  - credential storage (mock keyring, test encrypted store)
  - path handling and config
  - cache rm/purge correctness
  - provider “dry run” (no network in tests; mock download layer)
- Integration tests (optional, marked):
  - small public dataset download via HTTP/HF

### Logging
- Use structured logging (rich output in CLI ok).
- Never log secret values.
- Provide `--verbose` and `--quiet`.

### Compatibility
- Python 3.11+ (unless strong reason).
- Must run on macOS, Linux, Windows.

---

## Implementation milestones (recommended)

### M0 — Skeleton
- CLI scaffold + config + dataset registry
- `list`, `info`, `get` for at least one public provider (HF or UCI)
- Data layout + metadata writing

### M1 — Secure credentials
- Keyring backend
- Encrypted file backend (AES-256-GCM + Argon2id)
- `auth set/status/rm`
- Add Kaggle provider using stored creds

### M2 — Cache & purge polish
- Cache index + `ls/rm/purge`
- Size calculation
- `--json` outputs
- Clear docs + examples

### M3 — Retail benchmark pack
- Add 6–10 curated datasets spanning multiple retail topics
- Add `--prepare` normalization for 2–3 core datasets
- Provide demo notebooks (Bayes decisioning vs baseline)
- Create a good README.md for the project with nearly all info from this AGENTS.md file

---

## “Stop and ask” checklist for decisions

Before adding complexity, ask:
1) Does this reduce thinking for the user?
2) Does it improve security or correctness meaningfully?
3) Is there a simpler default that covers 80%?
4) Can the user recover easily (rm/purge/reset)?
5) Is this legally/ethically OK to distribute?

---

## Developer commands (uv)

- Create env:
  - `uv venv`
  - `uv pip install -e ".[dev]"`
- Run tests:
  - `uv run pytest -q`
- Run CLI locally:
  - `uv run retaildata --help`

---

## Notes on credentials in CI

Support env vars:
- `RETAILDATA_KAGGLE_USERNAME`
- `RETAILDATA_KAGGLE_KEY`

Priority order:
1) env vars (CI)
2) keyring
3) encrypted store

Never write env-provided secrets to disk unless user explicitly calls `auth set`.

---

## License & dataset attribution

`retaildata` must:
- include dataset source attribution in `metadata.json`
- respect dataset-specific licenses and access terms
- never redistribute restricted datasets inside the package
