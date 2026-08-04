#!/usr/bin/env python3
"""
Reliable EVM native-coin batch sender v3.

Design goals:
- one payment is never silently re-sent with a new nonce after broadcasting;
- transactions from the same source address are serialized;
- EIP-1559 and legacy fee support with explicit fee caps;
- append-only JSONL journal (no O(n²) rewrites);
- crash-safe resumable runs using deterministic payment IDs and pre-broadcast journaling;
- graceful shutdown and clear "pending/unknown" outcomes;
- private keys are never written to logs or result files.

Input wallets.json example:
[
  {
    "from_address": "0x...",
    "to_address": "0x...",
    "private_key": "0x...",
    "value": "0.01",
    "id": "optional-external-id"
  }
]
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import logging
import os
import random
import signal
import threading
import time
from dataclasses import asdict, dataclass, field, replace
from decimal import Decimal, InvalidOperation, ROUND_CEILING
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from web3 import Web3
from web3.exceptions import TransactionNotFound
from web3.providers.rpc import HTTPProvider


# =============================================================================
# CONFIG
# =============================================================================


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be one of: 1/0, true/false, yes/no, on/off")


@dataclass(frozen=True)
class Config:
    # Network
    chain_id: int = int(os.getenv("CHAIN_ID", "1"))
    network: str = os.getenv("NETWORK", "mainnet")
    rpc_url: Optional[str] = os.getenv("RPC_URL")
    infura_project_id: Optional[str] = os.getenv("INFURA_PROJECT_ID")

    # Concurrency
    max_workers: int = int(os.getenv("MAX_WORKERS", "20"))

    # Sending / replacement
    max_replacements: int = int(os.getenv("MAX_REPLACEMENTS", "4"))
    replacement_bump_percent: int = int(os.getenv("REPLACEMENT_BUMP_PERCENT", "15"))
    min_bump_gwei: Decimal = Decimal(os.getenv("MIN_BUMP_GWEI", "1"))
    broadcast_retries: int = int(os.getenv("BROADCAST_RETRIES", "3"))

    # Fees
    priority_fee_gwei: Decimal = Decimal(os.getenv("PRIORITY_FEE_GWEI", "2"))
    max_fee_multiplier: Decimal = Decimal(os.getenv("MAX_FEE_MULTIPLIER", "2"))
    max_fee_cap_gwei: Decimal = Decimal(os.getenv("MAX_FEE_CAP_GWEI", "200"))

    # Gas
    gas_safety_multiplier: Decimal = Decimal(os.getenv("GAS_SAFETY_MULTIPLIER", "1.15"))
    allow_gas_fallback: bool = env_bool("ALLOW_GAS_FALLBACK", False)
    gas_limit_fallback: int = int(os.getenv("GAS_LIMIT_FALLBACK", "21000"))

    # Receipt / pending tracking
    receipt_timeout: float = float(os.getenv("RECEIPT_TIMEOUT", "180"))
    receipt_poll: float = float(os.getenv("RECEIPT_POLL", "2"))
    confirmations: int = int(os.getenv("CONFIRMATIONS", "2"))
    final_pending_wait: float = float(os.getenv("FINAL_PENDING_WAIT", "30"))

    # RPC
    rpc_timeout: float = float(os.getenv("RPC_TIMEOUT", "30"))
    rpc_retries: int = int(os.getenv("RPC_RETRIES", "3"))
    session_pool_size: int = int(os.getenv("SESSION_POOL_SIZE", "100"))

    # Files
    wallets_file: str = os.getenv("WALLETS_FILE", "wallets.json")
    journal_file: str = os.getenv("JOURNAL_FILE", "sender_journal.jsonl")
    summary_file: str = os.getenv("SUMMARY_FILE", "sender_summary.json")

    # Runtime / safety
    dry_run: bool = env_bool("DRY_RUN", False)
    resume: bool = env_bool("RESUME", True)
    fail_on_duplicate_payment: bool = env_bool("FAIL_ON_DUPLICATE_PAYMENT", True)
    retry_unresolved: bool = env_bool("RETRY_UNRESOLVED", False)
    log_level: str = os.getenv("LOG_LEVEL", "INFO").upper()

    def validate(self) -> None:
        if self.chain_id <= 0:
            raise ValueError("CHAIN_ID must be positive")
        if self.max_workers <= 0:
            raise ValueError("MAX_WORKERS must be positive")
        if self.max_replacements < 0:
            raise ValueError("MAX_REPLACEMENTS cannot be negative")
        if self.broadcast_retries <= 0:
            raise ValueError("BROADCAST_RETRIES must be positive")
        if self.replacement_bump_percent < 10:
            raise ValueError("REPLACEMENT_BUMP_PERCENT should be at least 10")
        if self.min_bump_gwei <= 0:
            raise ValueError("MIN_BUMP_GWEI must be positive")
        if self.priority_fee_gwei < 0:
            raise ValueError("PRIORITY_FEE_GWEI cannot be negative")
        if self.max_fee_multiplier < 1:
            raise ValueError("MAX_FEE_MULTIPLIER must be >= 1")
        if self.max_fee_cap_gwei <= 0:
            raise ValueError("MAX_FEE_CAP_GWEI must be positive")
        if self.gas_safety_multiplier < 1:
            raise ValueError("GAS_SAFETY_MULTIPLIER must be >= 1")
        if self.receipt_timeout <= 0 or self.receipt_poll <= 0:
            raise ValueError("receipt timeout/poll values must be positive")
        if self.confirmations <= 0:
            raise ValueError("CONFIRMATIONS must be positive")


CONFIG = Config()


# =============================================================================
# LOGGING / GLOBAL STATE
# =============================================================================

logging.basicConfig(
    level=getattr(logging, CONFIG.log_level, logging.INFO),
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
)
logger = logging.getLogger("evm_sender")

STOP = threading.Event()
JOURNAL_LOCK = threading.Lock()
SUMMARY_LOCK = threading.Lock()
THREAD_LOCAL = threading.local()
ADDRESS_LOCKS: dict[str, threading.Lock] = {}
ADDRESS_LOCKS_GUARD = threading.Lock()


# =============================================================================
# MODELS
# =============================================================================

@dataclass(frozen=True)
class Payment:
    payment_id: str
    source_index: int
    from_address: str
    to_address: str
    private_key: str = field(repr=False)
    value_eth: Decimal
    value_wei: int


@dataclass(frozen=True)
class Outcome:
    payment_id: str
    ok: bool
    state: str
    reason: str
    from_address: str
    to_address: str
    value_eth: str
    value_wei: int
    nonce: Optional[int] = None
    tx_hash: Optional[str] = None
    tx_hashes: tuple[str, ...] = ()
    receipt: Optional[dict[str, Any]] = None
    dry_run_tx: Optional[dict[str, Any]] = None
    elapsed_seconds: Optional[float] = None
    timestamp: float = field(default_factory=time.time)


# =============================================================================
# HELPERS
# =============================================================================


def interruptible_sleep(seconds: float) -> bool:
    """Return True when interrupted by STOP."""
    return STOP.wait(max(0.0, seconds))


def backoff(attempt: int, cap: float = 20.0) -> float:
    return min(cap, 0.75 * (2**attempt)) + random.uniform(0.05, 0.5)


def short(address: str) -> str:
    return f"{address[:6]}...{address[-4:]}" if len(address) >= 12 else address


def jsonable(value: Any) -> Any:
    if isinstance(value, bytes):
        return Web3.to_hex(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Mapping):
        return {str(k): jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [jsonable(v) for v in value]
    return value


def raw_transaction(signed_tx: Any) -> bytes:
    raw = getattr(signed_tx, "raw_transaction", None)
    if raw is None:
        raw = getattr(signed_tx, "rawTransaction", None)
    if raw is None:
        raise AttributeError("Signed transaction has no raw transaction field")
    return raw


def normalize_tx_hash(tx_hash: Any) -> str:
    value = Web3.to_hex(tx_hash)
    return value if value.startswith("0x") else f"0x{value}"


def gwei_to_wei(value: Decimal) -> int:
    return int((value * Decimal(10**9)).to_integral_value(rounding=ROUND_CEILING))


def ceil_decimal(value: Decimal) -> int:
    return int(value.to_integral_value(rounding=ROUND_CEILING))


def address_lock(address: str) -> threading.Lock:
    key = address.lower()
    with ADDRESS_LOCKS_GUARD:
        lock = ADDRESS_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            ADDRESS_LOCKS[key] = lock
        return lock


def payment_fingerprint(from_address: str, to_address: str, value_wei: int, external_id: str = "") -> str:
    canonical = f"{from_address.lower()}|{to_address.lower()}|{value_wei}|{external_id.strip()}"
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# =============================================================================
# RPC / WEB3 (THREAD-LOCAL SESSION)
# =============================================================================


def get_rpc_url(config: Config) -> str:
    if config.rpc_url:
        return config.rpc_url
    if config.infura_project_id:
        return f"https://{config.network}.infura.io/v3/{config.infura_project_id}"
    raise RuntimeError("Set RPC_URL or INFURA_PROJECT_ID")


def create_session(config: Config) -> requests.Session:
    retry = Retry(
        total=config.rpc_retries,
        connect=config.rpc_retries,
        read=config.rpc_retries,
        status=config.rpc_retries,
        backoff_factor=0.4,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"POST"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(
        pool_connections=config.session_pool_size,
        pool_maxsize=config.session_pool_size,
        max_retries=retry,
        pool_block=True,
    )
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def make_provider(config: Config, session: requests.Session) -> HTTPProvider:
    # web3.py v6/v7 accepts `session=`. Older releases may not, so fall back.
    try:
        return HTTPProvider(
            endpoint_uri=get_rpc_url(config),
            request_kwargs={"timeout": config.rpc_timeout},
            session=session,
        )
    except TypeError:
        return HTTPProvider(
            endpoint_uri=get_rpc_url(config),
            request_kwargs={"timeout": config.rpc_timeout},
        )


def web3(config: Config) -> Web3:
    client = getattr(THREAD_LOCAL, "web3", None)
    if client is not None:
        return client

    session = create_session(config)
    client = Web3(make_provider(config, session))
    if not client.is_connected():
        session.close()
        raise ConnectionError("Unable to connect to RPC")

    actual_chain_id = int(client.eth.chain_id)
    if actual_chain_id != config.chain_id:
        session.close()
        raise RuntimeError(
            f"Wrong chain connected: expected={config.chain_id}, got={actual_chain_id}"
        )

    THREAD_LOCAL.session = session
    THREAD_LOCAL.web3 = client
    return client


def verify_connection(config: Config) -> None:
    client = web3(config)
    latest = int(client.eth.block_number)
    logger.info(
        "Connected | chain_id=%s | network=%s | latest_block=%s",
        config.chain_id,
        config.network,
        latest,
    )


# =============================================================================
# JOURNAL / RESUME
# =============================================================================


def append_jsonl(path: Path, record: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(jsonable(dict(record)), ensure_ascii=False, separators=(",", ":"))
    with JOURNAL_LOCK:
        with path.open("a", encoding="utf-8", newline="\n") as fh:
            fh.write(line)
            fh.write("\n")
            fh.flush()
            os.fsync(fh.fileno())


def load_resume_sets(path: Path) -> tuple[set[str], set[str]]:
    """Return (completed, unresolved) payment IDs from the latest journal record.

    Any payment that reached the signed/broadcast stage is conservatively blocked from
    automatic resend until it has a terminal confirmed/dry-run/reverted record. This
    prevents a restart from silently paying twice with a fresh nonce.
    """
    latest: dict[str, dict[str, Any]] = {}
    if not path.exists():
        return set(), set()

    with path.open("r", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                logger.warning("Ignoring invalid journal line %s", line_no)
                continue
            payment_id = item.get("payment_id")
            if payment_id:
                latest[str(payment_id)] = item

    completed: set[str] = set()
    unresolved: set[str] = set()
    unresolved_states = {"signed", "broadcast", "pending", "unknown"}
    for payment_id, item in latest.items():
        state = str(item.get("state", ""))
        tx_hash = item.get("tx_hash") or item.get("expected_hash")
        if state in {"confirmed", "dry_run"}:
            completed.add(payment_id)
        elif state in unresolved_states or (tx_hash and state not in {"reverted"}):
            unresolved.add(payment_id)
    return completed, unresolved


def append_attempt_event(
    config: Config,
    payment: Payment,
    *,
    state: str,
    nonce: int,
    tx_hash: str,
    fees: Mapping[str, int],
    gas: int,
    replacement_index: int,
    detail: str,
) -> None:
    """Persist transaction identity before/after RPC broadcast.

    The raw transaction and private key are intentionally never stored.
    """
    append_jsonl(
        Path(config.journal_file),
        {
            "record_type": "attempt",
            "payment_id": payment.payment_id,
            "state": state,
            "detail": detail,
            "from_address": payment.from_address,
            "to_address": payment.to_address,
            "value_eth": str(payment.value_eth),
            "value_wei": payment.value_wei,
            "nonce": nonce,
            "tx_hash": tx_hash,
            "fees": dict(fees),
            "gas": gas,
            "replacement_index": replacement_index,
            "timestamp": time.time(),
        },
    )


def atomic_write_json(path: Path, data: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temp.open("w", encoding="utf-8", newline="\n") as fh:
        json.dump(jsonable(dict(data)), fh, ensure_ascii=False, indent=2)
        fh.write("\n")
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(temp, path)


# =============================================================================
# INPUT VALIDATION
# =============================================================================


def parse_payment(item: Any, index: int, config: Config) -> Payment:
    if not isinstance(item, dict):
        raise ValueError("item must be a JSON object")

    required = ("from_address", "to_address", "private_key", "value")
    missing = [key for key in required if key not in item]
    if missing:
        raise ValueError(f"missing fields: {', '.join(missing)}")

    client = web3(config)
    try:
        from_address = Web3.to_checksum_address(str(item["from_address"]).strip())
        to_address = Web3.to_checksum_address(str(item["to_address"]).strip())
    except Exception as exc:
        raise ValueError(f"invalid address: {exc}") from exc

    try:
        value_eth = Decimal(str(item["value"]).strip())
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"invalid value: {item.get('value')!r}") from exc
    if not value_eth.is_finite() or value_eth <= 0:
        raise ValueError("value must be a finite positive number")

    value_wei_decimal = value_eth * Decimal(10**18)
    if value_wei_decimal != value_wei_decimal.to_integral_value():
        raise ValueError("value has more than 18 decimal places")
    value_wei = int(value_wei_decimal)

    private_key = str(item["private_key"]).strip()
    try:
        account = client.eth.account.from_key(private_key)
    except Exception as exc:
        raise ValueError("invalid private key") from exc
    if account.address.lower() != from_address.lower():
        raise ValueError("private key does not match from_address")

    external_id = str(item.get("id", "")).strip()
    payment_id = external_id or payment_fingerprint(from_address, to_address, value_wei)

    return Payment(
        payment_id=payment_id,
        source_index=index,
        from_address=from_address,
        to_address=to_address,
        private_key=private_key,
        value_eth=value_eth,
        value_wei=value_wei,
    )


def load_payments(config: Config) -> list[Payment]:
    path = Path(config.wallets_file)
    if not path.exists():
        raise FileNotFoundError(f"Wallet file not found: {path}")

    with path.open("r", encoding="utf-8-sig") as fh:
        raw = json.load(fh, parse_float=Decimal)
    if not isinstance(raw, list):
        raise ValueError("wallets file must contain a JSON list")

    payments: list[Payment] = []
    errors = 0
    for index, item in enumerate(raw):
        try:
            payments.append(parse_payment(item, index, config))
        except Exception as exc:
            errors += 1
            logger.error("Invalid payment index=%s: %s", index, exc)

    duplicates: dict[str, list[int]] = {}
    for payment in payments:
        duplicates.setdefault(payment.payment_id, []).append(payment.source_index)
    duplicate_groups = {pid: indexes for pid, indexes in duplicates.items() if len(indexes) > 1}
    if duplicate_groups:
        message = "; ".join(f"{pid[:12]}... at indexes {indexes}" for pid, indexes in duplicate_groups.items())
        if config.fail_on_duplicate_payment:
            raise ValueError(f"Duplicate payment IDs detected: {message}")
        logger.warning("Duplicate payment IDs detected; keeping first: %s", message)
        seen: set[str] = set()
        payments = [p for p in payments if not (p.payment_id in seen or seen.add(p.payment_id))]

    logger.info("Loaded %s valid payments (%s invalid)", len(payments), errors)
    return payments


# =============================================================================
# FEES / TRANSACTION BUILDING
# =============================================================================


def fee_cap_wei(config: Config) -> int:
    return gwei_to_wei(config.max_fee_cap_gwei)


def enforce_fee_cap(fees: Mapping[str, int], config: Config) -> None:
    effective = int(fees.get("maxFeePerGas", fees.get("gasPrice", 0)))
    if effective > fee_cap_wei(config):
        raise RuntimeError(
            f"fee cap exceeded: proposed={Web3.from_wei(effective, 'gwei')} gwei, "
            f"cap={config.max_fee_cap_gwei} gwei"
        )


def initial_fees(client: Web3, config: Config) -> dict[str, int]:
    latest = client.eth.get_block("latest")
    base_fee = latest.get("baseFeePerGas")
    if base_fee is None:
        fees = {"gasPrice": int(client.eth.gas_price)}
    else:
        base_fee_int = int(base_fee)
        priority = gwei_to_wei(config.priority_fee_gwei)
        max_fee = ceil_decimal(Decimal(base_fee_int) * config.max_fee_multiplier) + priority
        fees = {
            "type": 2,
            "maxPriorityFeePerGas": priority,
            "maxFeePerGas": max_fee,
        }
    enforce_fee_cap(fees, config)
    return fees


def bump_fees(client: Web3, previous: Mapping[str, int], config: Config) -> dict[str, int]:
    factor = Decimal(100 + config.replacement_bump_percent) / Decimal(100)
    minimum_bump = gwei_to_wei(config.min_bump_gwei)

    if "gasPrice" in previous:
        old = int(previous["gasPrice"])
        fees = {"gasPrice": max(ceil_decimal(Decimal(old) * factor), old + minimum_bump)}
        enforce_fee_cap(fees, config)
        return fees

    latest = client.eth.get_block("latest")
    base_fee = int(latest.get("baseFeePerGas") or 0)
    old_priority = int(previous["maxPriorityFeePerGas"])
    old_max = int(previous["maxFeePerGas"])
    priority = max(ceil_decimal(Decimal(old_priority) * factor), old_priority + minimum_bump)
    max_fee = max(
        ceil_decimal(Decimal(old_max) * factor),
        old_max + minimum_bump,
        ceil_decimal(Decimal(base_fee) * config.max_fee_multiplier) + priority,
    )
    fees = {
        "type": 2,
        "maxPriorityFeePerGas": priority,
        "maxFeePerGas": max_fee,
    }
    enforce_fee_cap(fees, config)
    return fees


def estimate_gas(client: Web3, tx: Mapping[str, Any], config: Config) -> int:
    try:
        estimated = int(client.eth.estimate_gas(dict(tx)))
    except Exception as exc:
        if not config.allow_gas_fallback:
            raise RuntimeError(f"gas estimation failed: {exc}") from exc
        logger.warning("Gas estimation failed; using fallback=%s: %s", config.gas_limit_fallback, exc)
        estimated = config.gas_limit_fallback

    return max(21_000, ceil_decimal(Decimal(estimated) * config.gas_safety_multiplier))


def build_transaction(
    client: Web3,
    payment: Payment,
    nonce: int,
    fees: Mapping[str, int],
    config: Config,
) -> dict[str, Any]:
    tx: dict[str, Any] = {
        "chainId": config.chain_id,
        "nonce": nonce,
        "from": payment.from_address,
        "to": payment.to_address,
        "value": payment.value_wei,
        **dict(fees),
    }
    tx["gas"] = estimate_gas(client, tx, config)
    return tx


def maximum_cost(tx: Mapping[str, Any]) -> int:
    fee_per_gas = int(tx.get("maxFeePerGas", tx.get("gasPrice", 0)))
    return int(tx["value"]) + int(tx["gas"]) * fee_per_gas


# =============================================================================
# RECEIPT / BROADCAST SAFETY
# =============================================================================


def get_receipt(client: Web3, tx_hash: str) -> Optional[dict[str, Any]]:
    try:
        return jsonable(dict(client.eth.get_transaction_receipt(tx_hash)))
    except TransactionNotFound:
        return None


def receipt_confirmations(client: Web3, receipt: Mapping[str, Any]) -> int:
    return int(client.eth.block_number) - int(receipt["blockNumber"]) + 1


def wait_for_receipt(
    client: Web3,
    tx_hashes: Iterable[str],
    timeout: float,
    config: Config,
) -> tuple[Optional[str], Optional[dict[str, Any]]]:
    hashes = list(dict.fromkeys(tx_hashes))
    deadline = time.monotonic() + timeout

    while not STOP.is_set() and time.monotonic() < deadline:
        for tx_hash in reversed(hashes):
            try:
                receipt = get_receipt(client, tx_hash)
                if receipt and receipt_confirmations(client, receipt) >= config.confirmations:
                    return tx_hash, receipt
            except Exception as exc:
                logger.warning("Receipt poll failed for %s: %s", tx_hash, exc)
        interruptible_sleep(config.receipt_poll)
    return None, None


def nonce_consumed(client: Web3, address: str, nonce: int) -> bool:
    # latest > nonce means a transaction with this nonce is already mined.
    return int(client.eth.get_transaction_count(address, "latest")) > nonce


def nonce_present_in_pending_chain(client: Web3, address: str, nonce: int) -> bool:
    return int(client.eth.get_transaction_count(address, "pending")) > nonce


def broadcast_signed(
    client: Web3,
    raw_tx: bytes,
    expected_hash: str,
    config: Config,
) -> str:
    last_error: Optional[Exception] = None
    for attempt in range(config.broadcast_retries):
        if STOP.is_set():
            raise InterruptedError("shutdown requested")
        try:
            returned = normalize_tx_hash(client.eth.send_raw_transaction(raw_tx))
            if returned.lower() != expected_hash.lower():
                raise RuntimeError(f"RPC returned unexpected tx hash {returned}, expected {expected_hash}")
            return returned
        except Exception as exc:
            last_error = exc
            message = str(exc).lower()
            if "already known" in message or "known transaction" in message or "already imported" in message:
                return expected_hash
            # A timeout may happen after RPC accepted the tx. Check by deterministic hash.
            try:
                if client.eth.get_transaction(expected_hash):
                    return expected_hash
            except Exception:
                pass
            if "nonce too low" in message:
                raise
            if attempt + 1 < config.broadcast_retries:
                interruptible_sleep(backoff(attempt))
    assert last_error is not None
    raise last_error


# =============================================================================
# SEND LOGIC
# =============================================================================


def make_outcome(payment: Payment, started: float, **kwargs: Any) -> Outcome:
    return Outcome(
        payment_id=payment.payment_id,
        from_address=payment.from_address,
        to_address=payment.to_address,
        value_eth=str(payment.value_eth),
        value_wei=payment.value_wei,
        elapsed_seconds=round(time.monotonic() - started, 3),
        **kwargs,
    )


def send_payment(payment: Payment, config: Config) -> Outcome:
    started = time.monotonic()
    client = web3(config)

    # Serializing by source address eliminates local nonce races and balance races.
    with address_lock(payment.from_address):
        if STOP.is_set():
            return make_outcome(payment, started, ok=False, state="stopped", reason="shutdown_requested")

        nonce: Optional[int] = None
        tx_hashes: list[str] = []
        try:
            nonce = int(client.eth.get_transaction_count(payment.from_address, "pending"))
            fees = initial_fees(client, config)

            for replacement_index in range(config.max_replacements + 1):
                if STOP.is_set():
                    return make_outcome(
                        payment,
                        started,
                        ok=False,
                        state="pending" if tx_hashes else "stopped",
                        reason="shutdown_requested_after_broadcast" if tx_hashes else "shutdown_requested",
                        nonce=nonce,
                        tx_hash=tx_hashes[-1] if tx_hashes else None,
                        tx_hashes=tuple(tx_hashes),
                    )

                if replacement_index > 0:
                    fees = bump_fees(client, fees, config)

                tx = build_transaction(client, payment, nonce, fees, config)
                balance = int(client.eth.get_balance(payment.from_address, "pending"))
                required = maximum_cost(tx)
                if balance < required:
                    return make_outcome(
                        payment,
                        started,
                        ok=False,
                        state="failed",
                        reason=f"insufficient_funds: need={required}, have={balance}",
                        nonce=nonce,
                        tx_hash=tx_hashes[-1] if tx_hashes else None,
                        tx_hashes=tuple(tx_hashes),
                    )

                if config.dry_run:
                    return make_outcome(
                        payment,
                        started,
                        ok=True,
                        state="dry_run",
                        reason="transaction_built_not_broadcast",
                        nonce=nonce,
                        dry_run_tx=jsonable(tx),
                    )

                signed = client.eth.account.sign_transaction(tx, payment.private_key)
                raw = raw_transaction(signed)
                expected_hash = normalize_tx_hash(Web3.keccak(raw))

                # Persist the deterministic transaction identity *before* touching RPC.
                # A process crash after RPC acceptance can therefore never erase the hash.
                if expected_hash not in tx_hashes:
                    tx_hashes.append(expected_hash)
                append_attempt_event(
                    config,
                    payment,
                    state="signed",
                    nonce=nonce,
                    tx_hash=expected_hash,
                    fees=fees,
                    gas=int(tx["gas"]),
                    replacement_index=replacement_index,
                    detail="signed_before_broadcast",
                )

                try:
                    tx_hash = broadcast_signed(client, raw, expected_hash, config)
                    append_attempt_event(
                        config,
                        payment,
                        state="broadcast",
                        nonce=nonce,
                        tx_hash=tx_hash,
                        fees=fees,
                        gas=int(tx["gas"]),
                        replacement_index=replacement_index,
                        detail="rpc_acknowledged_or_transaction_known",
                    )
                except Exception as exc:
                    message = str(exc).lower()

                    # Critical safety rule: never switch to a fresh nonce for the same payment.
                    # If this nonce has moved, the original transaction may have been accepted.
                    if "nonce too low" in message:
                        found_hash, receipt = wait_for_receipt(
                            client,
                            [*tx_hashes, expected_hash],
                            config.final_pending_wait,
                            config,
                        )
                        if receipt:
                            status = int(receipt.get("status", 0))
                            return make_outcome(
                                payment,
                                started,
                                ok=status == 1,
                                state="confirmed" if status == 1 else "reverted",
                                reason="confirmed_after_nonce_too_low" if status == 1 else "transaction_reverted",
                                nonce=nonce,
                                tx_hash=found_hash,
                                tx_hashes=tuple(dict.fromkeys([*tx_hashes, expected_hash])),
                                receipt=receipt,
                            )
                        if nonce_consumed(client, payment.from_address, nonce):
                            return make_outcome(
                                payment,
                                started,
                                ok=False,
                                state="unknown",
                                reason="nonce_consumed_but_receipt_not_found; do_not_resend_automatically",
                                nonce=nonce,
                                tx_hash=tx_hashes[-1] if tx_hashes else expected_hash,
                                tx_hashes=tuple(dict.fromkeys([*tx_hashes, expected_hash])),
                            )
                    raise

                if tx_hash not in tx_hashes:
                    tx_hashes.append(tx_hash)
                logger.info(
                    "%s -> %s | value=%s | nonce=%s | broadcast=%s/%s | hash=%s",
                    short(payment.from_address),
                    short(payment.to_address),
                    payment.value_eth,
                    nonce,
                    replacement_index + 1,
                    config.max_replacements + 1,
                    tx_hash,
                )

                found_hash, receipt = wait_for_receipt(
                    client,
                    tx_hashes,
                    config.receipt_timeout,
                    config,
                )
                if receipt:
                    status = int(receipt.get("status", 0))
                    return make_outcome(
                        payment,
                        started,
                        ok=status == 1,
                        state="confirmed" if status == 1 else "reverted",
                        reason="confirmed" if status == 1 else "transaction_reverted",
                        nonce=nonce,
                        tx_hash=found_hash,
                        tx_hashes=tuple(tx_hashes),
                        receipt=receipt,
                    )

                # If another process replaced/mined this nonce, do not manufacture a new payment.
                if nonce_consumed(client, payment.from_address, nonce):
                    found_hash, receipt = wait_for_receipt(
                        client,
                        tx_hashes,
                        config.final_pending_wait,
                        config,
                    )
                    if receipt:
                        status = int(receipt.get("status", 0))
                        return make_outcome(
                            payment,
                            started,
                            ok=status == 1,
                            state="confirmed" if status == 1 else "reverted",
                            reason="confirmed_after_delayed_receipt" if status == 1 else "transaction_reverted",
                            nonce=nonce,
                            tx_hash=found_hash,
                            tx_hashes=tuple(tx_hashes),
                            receipt=receipt,
                        )
                    return make_outcome(
                        payment,
                        started,
                        ok=False,
                        state="unknown",
                        reason="nonce_mined_but_known_receipt_missing; manual_check_required",
                        nonce=nonce,
                        tx_hash=tx_hashes[-1],
                        tx_hashes=tuple(tx_hashes),
                    )

                if replacement_index < config.max_replacements:
                    logger.warning(
                        "%s | receipt timeout; replacing same nonce=%s with higher fee",
                        short(payment.from_address),
                        nonce,
                    )

            pending = nonce_present_in_pending_chain(client, payment.from_address, nonce)
            return make_outcome(
                payment,
                started,
                ok=False,
                state="pending" if pending else "unknown",
                reason=(
                    "replacement_limit_reached; transaction_still_pending"
                    if pending
                    else "replacement_limit_reached; transaction_not_visible"
                ),
                nonce=nonce,
                tx_hash=tx_hashes[-1] if tx_hashes else None,
                tx_hashes=tuple(tx_hashes),
            )

        except InterruptedError:
            return make_outcome(
                payment,
                started,
                ok=False,
                state="pending" if tx_hashes else "stopped",
                reason="shutdown_requested_after_broadcast" if tx_hashes else "shutdown_requested",
                nonce=nonce,
                tx_hash=tx_hashes[-1] if tx_hashes else None,
                tx_hashes=tuple(tx_hashes),
            )
        except Exception as exc:
            logger.exception("%s | payment failed: %s", short(payment.from_address), exc)
            return make_outcome(
                payment,
                started,
                ok=False,
                state="failed" if not tx_hashes else "unknown",
                reason=f"{type(exc).__name__}: {exc}",
                nonce=nonce,
                tx_hash=tx_hashes[-1] if tx_hashes else None,
                tx_hashes=tuple(tx_hashes),
            )


# =============================================================================
# RUNNER
# =============================================================================


def run(payments: list[Payment], config: Config) -> dict[str, Any]:
    journal_path = Path(config.journal_file)
    skipped_completed = 0
    skipped_unresolved = 0
    if config.resume:
        completed_ids, unresolved_ids = load_resume_sets(journal_path)
        before = len(payments)
        skipped_completed = sum(p.payment_id in completed_ids for p in payments)
        skipped_unresolved = sum(p.payment_id in unresolved_ids for p in payments)
        blocked = completed_ids | (set() if config.retry_unresolved else unresolved_ids)
        payments = [p for p in payments if p.payment_id not in blocked]
        logger.info(
            "Resume: skipped completed=%s unresolved=%s retry_unresolved=%s",
            skipped_completed,
            0 if config.retry_unresolved else skipped_unresolved,
            config.retry_unresolved,
        )
        if unresolved_ids and not config.retry_unresolved:
            logger.warning(
                "%s unresolved payment(s) were not resent automatically. "
                "Inspect journal/chain; use --retry-unresolved only after verification.",
                skipped_unresolved,
            )

    total = len(payments)
    counters: dict[str, int] = {}
    started = time.monotonic()

    if total == 0:
        logger.info("Nothing to send")
        return {"total": 0, "duration_seconds": 0.0, "states": {}, "skipped_completed": skipped_completed, "skipped_unresolved": skipped_unresolved}

    logger.info(
        "Starting | payments=%s | workers=%s | dry_run=%s | journal=%s",
        total,
        config.max_workers,
        config.dry_run,
        journal_path,
    )

    executor = concurrent.futures.ThreadPoolExecutor(
        max_workers=config.max_workers,
        thread_name_prefix="sender",
    )
    futures: dict[concurrent.futures.Future[Outcome], Payment] = {}
    try:
        for payment in payments:
            if STOP.is_set():
                break
            futures[executor.submit(send_payment, payment, config)] = payment

        completed = 0
        for future in concurrent.futures.as_completed(futures):
            payment = futures[future]
            try:
                outcome = future.result()
            except Exception as exc:
                logger.exception("Worker crashed for payment=%s: %s", payment.payment_id, exc)
                outcome = make_outcome(
                    payment,
                    time.monotonic(),
                    ok=False,
                    state="failed",
                    reason=f"worker_crashed: {type(exc).__name__}: {exc}",
                )

            append_jsonl(journal_path, asdict(outcome))
            completed += 1
            counters[outcome.state] = counters.get(outcome.state, 0) + 1

            elapsed = time.monotonic() - started
            rate = completed / elapsed if elapsed > 0 else 0.0
            eta = (total - completed) / rate if rate > 0 else 0.0
            logger.info(
                "Progress %s/%s | state=%s | rate=%.2f/s | eta=%.1fs | counts=%s",
                completed,
                total,
                outcome.state,
                rate,
                eta,
                counters,
            )

            if STOP.is_set():
                for pending_future in futures:
                    pending_future.cancel()

    finally:
        executor.shutdown(wait=True, cancel_futures=True)

    summary = {
        "total": total,
        "duration_seconds": round(time.monotonic() - started, 3),
        "states": dict(sorted(counters.items())),
        "skipped_completed": skipped_completed,
        "skipped_unresolved": skipped_unresolved,
        "journal_file": str(journal_path),
        "stopped": STOP.is_set(),
        "timestamp": time.time(),
    }
    with SUMMARY_LOCK:
        atomic_write_json(Path(config.summary_file), summary)
    logger.info("Finished | %s", summary)
    return summary


# =============================================================================
# CLI / SHUTDOWN
# =============================================================================


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reliable EVM native-coin batch sender")
    parser.add_argument("--wallets", help="Input wallets JSON file")
    parser.add_argument("--journal", help="Append-only JSONL result journal")
    parser.add_argument("--summary", help="Run summary JSON file")
    parser.add_argument("--workers", type=int, help="Maximum worker count")
    parser.add_argument("--dry-run", action="store_true", help="Build and validate without broadcasting")
    parser.add_argument("--no-resume", action="store_true", help="Ignore journal resume protection")
    parser.add_argument(
        "--retry-unresolved",
        action="store_true",
        help="Allow resend of payments whose previous signed/broadcast attempt is unresolved (dangerous)",
    )
    parser.add_argument("--log-level", choices=("DEBUG", "INFO", "WARNING", "ERROR"))
    return parser.parse_args()


def apply_args(config: Config, args: argparse.Namespace) -> Config:
    updates: dict[str, Any] = {}
    if args.wallets:
        updates["wallets_file"] = args.wallets
    if args.journal:
        updates["journal_file"] = args.journal
    if args.summary:
        updates["summary_file"] = args.summary
    if args.workers is not None:
        updates["max_workers"] = args.workers
    if args.dry_run:
        updates["dry_run"] = True
    if args.no_resume:
        updates["resume"] = False
    if args.retry_unresolved:
        updates["retry_unresolved"] = True
    if args.log_level:
        updates["log_level"] = args.log_level
    return replace(config, **updates)


def shutdown_handler(signum: int, _frame: Any) -> None:
    logger.warning("Signal %s received; stopping safely", signum)
    STOP.set()


def install_signal_handlers() -> None:
    signal.signal(signal.SIGINT, shutdown_handler)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, shutdown_handler)


def main() -> int:
    install_signal_handlers()
    config = apply_args(CONFIG, parse_args())

    try:
        config.validate()
        logging.getLogger().setLevel(getattr(logging, config.log_level, logging.INFO))
        verify_connection(config)
        payments = load_payments(config)
        if not payments:
            logger.error("No valid payments loaded")
            return 2
        summary = run(payments, config)
        # Pending/unknown/reverted/failed are intentionally non-zero.
        bad = sum(summary["states"].get(state, 0) for state in ("failed", "reverted", "unknown", "pending", "stopped"))
        return 1 if bad else 0
    except KeyboardInterrupt:
        STOP.set()
        logger.warning("Interrupted")
        return 130
    except Exception as exc:
        logger.exception("Fatal error: %s", exc)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
