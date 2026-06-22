#!/usr/bin/env python3
"""
N2B MCP Server — stdio transport
Provides email validation, MX record lookup, and PostgreSQL persistence tools.
"""

import asyncio
import json
import re
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from fastmcp import FastMCP

# ── Load Environment Configuration ────────────────────────────────────────────────
current_dir = Path(__file__).resolve().parent
load_dotenv(dotenv_path=current_dir / ".env")
load_dotenv(dotenv_path=current_dir.parent / ".env")

# ── Constants ──────────────────────────────────────────────────────────────────────
VALIDATE_EMAIL_URL = "https://agentesapi.27x.ai/validate-email"
VALIDATE_EMAIL_RESULT_URL = "https://agentesapi.27x.ai/validate-email/result"
DB_URL = "postgresql://n2b_user:VI8k3lf9JcU9otl2O8Rq736Vraug1rko@dpg-d8pnk4jtqb8s738c261g-a.oregon-postgres.render.com/n2b"

# FIX 1: Added the missing comma between "ward.howell@withclutch.com" and
# "adam.adam@moniepoint.com". Without it Python silently concatenated them into
# one invalid email address, losing adam.adam@moniepoint.com entirely and
# reducing the list from 68 to 67 entries.
EMAILS_LIST: List[str] = [
    "pkim@ioufinancial.com",
    "edwin.vargas@roadsync.com",
    "bruce.greeson@roadsync.com",
    "ward.howell@withclutch.com",       # <-- comma was missing after this line
    "adam.adam@moniepoint.com",         # <-- this entry was being swallowed
    "nitu.kalyani@pw.live",
    "ramya.ghulati@pw.live",
    "devashree.bartaria@pw.live",
    "travis.eldik@frazierdeeter.com",
    "jerryy@prmg.net",
    "anorman@guildmortgage.net",
    "sharon.w@elementfunding.com",
    "scottw@cnanational.com",
    "katherineh@daveramsey.com",
    "cline@cuofga.org",
    "agbontaen.ann@banksouthmortgage.com",
    "brownerica@firstcommand.com",
    "asensookyere@unitedfidelity.com",
    "blake.adams@frazierdeeter.com",
    "keoni.liang@andersen.com",
    "evan.troutt@frazierdeeter.com",
    "maurice.nichols@fifsg.com",
    "russell.peggues@fifsg.com",
    "dblankenship@renasant.com",
    "anastasia.jordan@lendmarkfinancial.com",
    "kelsea.white@lbmc.com",
    "ashley.shaffer@frazierdeeter.com",
    "elijah.briscoe@frazierdeeter.com",
    "martin.magee@mcaleer-rushe.co.uk",
    "bruna.arcibelli@fhb.com",
    "jake.hijirida@fhb.com",
    "anthony.wong@fhb.com",
    "joanna.liu@fhb.com",
    "nicholas.bottom@fhb.com",
    "ben.kashiwabara@fhb.com",
    "craymond@fhb.com",
    "davidson@fhb.com",
    "gary.yu@fhb.com",
    "chev.kodama@fhb.com",
    "bernadette.andrews@amerisbank.com",
    "jeffrey.higashi@fhb.com",
    "brandon.aurelio@fhb.com",
    "alistair.cameron@fhb.com",
    "doc_h_elcc@yahoo.com",
    "debbieinks04@yahoo.com",
    "jami_colorado79@yahoo.com",
    "lynnguyen303@yahoo.com",
    "dgelles@yahoo.com",
    "jake_skow@yahoo.com",
    "lomeliw@yahoo.com",
    "n640@yahoo.com",
    "ejones1434@yahoo.com",
    "gbearly@yahoo.com",
    "dziroli@yahoo.com",
    "devon.hopkins@yahoo.com",
    "lazyrmei999@yahoo.com",
    "fshokouhi@yahoo.com",
    "hkhoshnevisan@yahoo.com",
    "bzmom45@yahoo.com",
    "szeivaz@yahoo.com",
    "stevechaijr@yahoo.com",
    "domlam28@yahoo.com",
    "alan.wolfer@yahoo.com",
    "chi_to@yahoo.com",
    "chi.to@yahoo.com",
    "ericallegakoen@yahoo.com",
    "aweemaes@yahoo.com",
    "whalen_amber@yahoo.com",
]

# FIX 5: Use actual list length instead of hardcoded 73.
EMAIL_COUNT = len(EMAILS_LIST)

# Maximum number of outer retry loops before giving up on unresolved emails.
# Inner poll already retries up to 7500 times (~4 hrs). This outer cap prevents
# a second infinite loop if the inner poll returns None for every remaining email.
MAX_OUTER_RETRIES = 3

# ── Initialize FastMCP Server ──────────────────────────────────────────────────────
mcp = FastMCP(name="N2B Utils", version="1.0.0")


# ── Database Helpers ───────────────────────────────────────────────────────────────
def get_db_connection():
    """Create and return a new PostgreSQL connection."""
    return psycopg2.connect(DB_URL)


def ensure_email_table(conn) -> None:
    """Create the email_validations table if it doesn't exist."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS email_validations (
                id SERIAL PRIMARY KEY,
                email TEXT NOT NULL UNIQUE,
                score NUMERIC,
                score_status TEXT,
                overall_status TEXT,
                is_valid BOOLEAN,
                mx_record_count INTEGER,
                mx_records JSONB,
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute(
            "ALTER TABLE email_validations ADD COLUMN IF NOT EXISTS mx_records JSONB;"
        )
    conn.commit()


def upsert_email_validation(
    conn,
    email: str,
    score=None,
    score_status: str = None,
    overall_status: str = None,
    is_valid: bool = None,
) -> None:
    """Upsert validation results for a single email into the DB.

    FIX 2: score now uses plain assignment (not COALESCE) so that a real score
    from the validation API always overwrites a previously-null placeholder row
    that was inserted by mx_record_save(). COALESCE is kept for the other fields
    so that MX-only rows retain their MX data when validation runs later.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO email_validations
                (email, score, score_status, overall_status, is_valid, updated_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (email) DO UPDATE SET
                score        = EXCLUDED.score,
                score_status = COALESCE(EXCLUDED.score_status,  email_validations.score_status),
                overall_status = COALESCE(EXCLUDED.overall_status, email_validations.overall_status),
                is_valid     = COALESCE(EXCLUDED.is_valid,       email_validations.is_valid),
                updated_at   = NOW()
            """,
            (email, score, score_status, overall_status, is_valid),
        )
    conn.commit()


def upsert_mx_records(
    conn, email: str, mx_count: int, mx_records: List[Dict[str, Any]]
) -> None:
    """Upsert MX record count and records for a single email into the DB."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO email_validations
                (email, mx_record_count, mx_records, updated_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (email) DO UPDATE SET
                mx_record_count = EXCLUDED.mx_record_count,
                mx_records      = EXCLUDED.mx_records,
                updated_at      = NOW()
            """,
            (email, mx_count, json.dumps(mx_records)),
        )
    conn.commit()


# ── Email Validation Helpers ───────────────────────────────────────────────────────
async def submit_validation_request(
    email: str, client: httpx.AsyncClient
) -> Optional[str]:
    """Submit an email for validation and return the tracking ID."""
    try:
        response = await client.post(
            VALIDATE_EMAIL_URL, json={"email": email}, timeout=30.0
        )
        if response.status_code == 200:
            data = response.json()
            tracking_id = (
                data.get("trackingId")
                or data.get("tracking_id")
                or data.get("id")
            )
            return tracking_id
        else:
            print(f"[WARN] Failed to submit {email}: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"[ERROR] Exception submitting {email}: {e}")
        return None


async def poll_validation_result(
    tracking_id: str, client: httpx.AsyncClient
) -> Optional[Dict[str, Any]]:
    """Poll the result endpoint for a single tracking ID until it reaches a final state.

    ROOT CAUSE FIX: the previous version returned as soon as overallStatus/status
    matched a "final" string (e.g. "completed"), even when the API had not yet
    populated `score`. agentesapi.27x.ai can report overallStatus="Completed"
    while score is still null — meaning the response LOOKED final but carried no
    usable data. Every downstream consumer then treated that None score as
    "this email is permanently done; never check it again," which is how nulls
    kept reappearing no matter how the upsert logic was patched.

    Fix: a response is only treated as truly final once a non-null score is
    present, OR the API reports an explicit failure/error/invalid state (which
    legitimately has no score and never will). A "completed" status with no
    score yet is treated as still-pending and polling continues — but capped by
    its own, shorter stall budget (STALL_MAX_ATTEMPTS) so an email that is
    genuinely stuck in "completed, no score" surfaces in minutes rather than
    consuming the full multi-hour polling window meant for normal processing.
    """
    failure_statuses = {"failed", "invalid", "error"}
    pending_like_statuses = {"completed", "done", "valid", "success"}
    max_attempts = 7500
    delay = 2  # seconds between polls

    # Once we've seen "completed but score is null" this many times in a row,
    # give up early rather than burning the full max_attempts budget — this
    # state means the API itself is stuck, not that we need to wait longer.
    stall_max_attempts = 30  # ~60s of seeing the same stalled state
    stall_count = 0

    for attempt in range(max_attempts):
        try:
            response = await client.get(
                VALIDATE_EMAIL_RESULT_URL,
                params={"trackingId": tracking_id},
                timeout=60.0,
                follow_redirects=True,
            )
            if response.status_code == 200:
                data = response.json()
                overall_status = str(data.get("overallStatus", "")).lower()
                status = str(data.get("status", "")).lower()
                has_score = data.get("score") is not None

                # Genuine terminal failure — there will never be a score, return now.
                if overall_status in failure_statuses or status in failure_statuses:
                    return data

                # Looks "completed" AND actually carries a score — truly final.
                if has_score and (overall_status in pending_like_statuses or status in pending_like_statuses):
                    return data

                # "Completed" but score is still null — API hasn't finished scoring.
                if (overall_status in pending_like_statuses or status in pending_like_statuses) and not has_score:
                    stall_count += 1
                    if stall_count == 1:
                        print(
                            f"[INFO] trackingId={tracking_id} reports "
                            f"'{overall_status or status}' but score is still null — "
                            "continuing to poll rather than accepting as final."
                        )
                    if stall_count >= stall_max_attempts:
                        print(
                            f"[WARN] trackingId={tracking_id} stuck in "
                            f"'{overall_status or status}' with no score after "
                            f"{stall_count} checks — giving up on this attempt."
                        )
                        return None
                else:
                    # Status isn't recognized as final or a known stall state —
                    # reset the stall counter since the response shape changed.
                    stall_count = 0

            await asyncio.sleep(delay)
        except Exception as e:
            print(f"[ERROR] Polling trackingId {tracking_id} attempt {attempt + 1}: {e}")
            await asyncio.sleep(delay)

    print(f"[WARN] Max polling attempts reached for trackingId {tracking_id} — score never populated")
    return None


# ── MX Record Helpers ──────────────────────────────────────────────────────────────
def extract_domain(email: str) -> str:
    """Extract the domain from an email address."""
    parts = email.split("@")
    return parts[-1] if len(parts) == 2 else email


def fetch_mx_records(domain: str) -> List[Dict[str, Any]]:
    """Fetch MX records for a domain using Google DNS over HTTPS."""
    if not re.match(r"^[a-zA-Z0-9._-]+$", domain):
        return []
    try:
        result = subprocess.run(
            ["curl", "-s", f"https://dns.google/resolve?name={domain}&type=MX"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode != 0:
            return []
        data = json.loads(result.stdout)
        answers = data.get("Answer", [])
        return answers if isinstance(answers, list) else []
    except Exception as e:
        print(f"[ERROR] MX lookup for {domain}: {e}")
        return []


# ── MCP Tool Implementations ───────────────────────────────────────────────────────

@mcp.tool(
    description=(
        f"Validates {EMAIL_COUNT} emails via the agentesapi.27x.ai validation endpoint. "
        "Submits all emails, collects tracking IDs, waits 1 minute before the first poll, "
        "then polls all remaining emails per pass. After each pass that still has unresolved "
        "emails a 1 minute wait is applied before the next retry. Max 5 outer retry attempts "
        "per email. Saves score and scoreStatus for each resolved email into PostgreSQL. "
        "NOTE: Run this AFTER mx_record_save to avoid the race condition where MX rows "
        "pre-populate score=null before validation completes."
    )
)
async def validate_mail_save() -> Dict[str, Any]:
    """Validate emails and persist results to the PostgreSQL database.

    Flow:
      1. Submit all emails → collect tracking IDs.
      2. Wait 1 minute before the very first poll pass.
      3. Poll every remaining email once per outer pass.
      4. After each pass that still has unresolved emails, wait 1 minute
         before the next retry.
      5. Give up on an email after 5 failed outer attempts (records it as Timeout).
      6. Skip upsert for any row whose score is still None so we never overwrite
         a previously saved real score.
    """
    OUTER_MAX_ATTEMPTS = 5          # max outer retry passes per email
    INITIAL_WAIT_SECONDS = 60      # 1-min wait before first poll
    RETRY_WAIT_SECONDS   = 60      # 1-min wait between subsequent passes

    emails = EMAILS_LIST[:EMAIL_COUNT]
    tracking_map: Dict[str, str] = {}

    async with httpx.AsyncClient() as client:
        # Step 1: Submit all emails and collect tracking IDs
        print(f"[INFO] Submitting {len(emails)} emails for validation...")
        for email in emails:
            tracking_id = await submit_validation_request(email, client)
            if tracking_id:
                tracking_map[email] = tracking_id
            await asyncio.sleep(0.1)

        print(
            f"[INFO] Collected {len(tracking_map)} tracking IDs. "
            f"Waiting {INITIAL_WAIT_SECONDS}s before first poll pass..."
        )
        await asyncio.sleep(INITIAL_WAIT_SECONDS)

        # Step 2: Outer loop — poll all remaining, retry up to OUTER_MAX_ATTEMPTS times
        results = []
        remaining = dict(tracking_map)
        outer_retry_counts: Dict[str, int] = {}
        pass_number = 0

        while remaining:
            pass_number += 1
            print(f"[INFO] Poll pass #{pass_number} — {len(remaining)} email(s) remaining...")
            next_remaining = {}

            for email, tracking_id in remaining.items():
                data = await poll_validation_result(tracking_id, client)
                if data:
                    results.append(
                        {
                            "email": data.get("email", email),
                            "score": data.get("score"),
                            "scoreStatus": data.get("scoreStatus"),
                            "overallStatus": data.get("overallStatus"),
                            "isValid": data.get("isValid"),
                        }
                    )
                else:
                    outer_retry_counts[email] = outer_retry_counts.get(email, 0) + 1
                    if outer_retry_counts[email] < OUTER_MAX_ATTEMPTS:
                        next_remaining[email] = tracking_id
                        print(
                            f"[INFO] {email} not resolved yet "
                            f"(attempt {outer_retry_counts[email]}/{OUTER_MAX_ATTEMPTS}) — will retry."
                        )
                    else:
                        print(
                            f"[WARN] Giving up on {email} after "
                            f"{OUTER_MAX_ATTEMPTS} outer attempts — recording as Timeout."
                        )
                        results.append(
                            {
                                "email": email,
                                "score": None,
                                "scoreStatus": "Pending",
                                "overallStatus": "Timeout",
                                "isValid": None,
                            }
                        )

            remaining = next_remaining
            if remaining:
                print(
                    f"[INFO] {len(remaining)} email(s) still pending — "
                    f"waiting {RETRY_WAIT_SECONDS}s before next pass..."
                )
                await asyncio.sleep(RETRY_WAIT_SECONDS)

    # Step 3: Persist results to PostgreSQL
    print(f"[INFO] Saving results to database...")
    conn = get_db_connection()
    saved = 0
    skipped = 0
    try:
        ensure_email_table(conn)
        for row in results:
            if row["score"] is None:
                print(f"[INFO] Skipping upsert for {row['email']} — score is null")
                skipped += 1
                continue
            upsert_email_validation(
                conn,
                email=row["email"],
                score=row["score"],
                score_status=row["scoreStatus"],
                overall_status=row["overallStatus"],
                is_valid=row["isValid"],
            )
            saved += 1
    finally:
        conn.close()

    return {
        "status": "success",
        "submitted": len(tracking_map),
        "saved": saved,
        "skipped_null_score": skipped,
        "results": results,
    }


@mcp.tool(
    description=(
        f"Looks up MX records for the domains of {EMAIL_COUNT} emails using Google DNS (dns.google). "
        "Stores the computed MX records and their count for each email in PostgreSQL. "
        "NOTE: Run this BEFORE validate_mail_save. Running it after is fine too, but "
        "running it in parallel risks pre-populating rows with score=null before "
        "validate_mail_save can write real scores."
    )
)
async def mx_record_save() -> Dict[str, Any]:
    """Find MX records for email domains and save the records to PostgreSQL."""
    emails = EMAILS_LIST[:EMAIL_COUNT]
    mx_results = []

    print(f"[INFO] Fetching MX records for {len(emails)} emails...")
    for email in emails:
        domain = extract_domain(email)
        answers = await asyncio.get_event_loop().run_in_executor(
            None, fetch_mx_records, domain
        )
        mx_entries = [
            {
                "name": answer.get("name"),
                "ttl": answer.get("TTL"),
                "data": answer.get("data"),
            }
            for answer in answers
        ]
        mx_count = len(mx_entries)
        mx_results.append(
            {
                "email": email,
                "domain": domain,
                "mx_count": mx_count,
                "mx_records": mx_entries,
            }
        )

    print(f"[INFO] Saving MX records to database...")
    conn = get_db_connection()
    try:
        ensure_email_table(conn)
        for row in mx_results:
            upsert_mx_records(conn, row["email"], row["mx_count"], row["mx_records"])
    finally:
        conn.close()

    return {
        "status": "success",
        "processed": len(mx_results),
        "results": mx_results,
    }


@mcp.tool(
    description=(
        f"Fetches the {EMAIL_COUNT} emails along with their validation scores, score statuses, "
        "MX records, and other fields from the PostgreSQL database."
    )
)
async def get_data() -> Dict[str, Any]:
    """Retrieve email validation and MX record data from the PostgreSQL database."""
    conn = get_db_connection()
    try:
        ensure_email_table(conn)
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                """
                SELECT email, score, score_status, overall_status, is_valid,
                       mx_record_count, mx_records, updated_at
                FROM email_validations
                ORDER BY id
                LIMIT %s
                """,
                (EMAIL_COUNT,),
            )
            rows = cur.fetchall()
            data = [
                {
                    "email": row["email"],
                    "score": float(row["score"]) if row["score"] is not None else None,
                    "scoreStatus": row["score_status"],
                    "overallStatus": row["overall_status"],
                    "isValid": row["is_valid"],
                    "mxRecordCount": row["mx_record_count"],
                    "mxRecords": row["mx_records"],
                    "updatedAt": row["updated_at"].isoformat()
                    if row["updated_at"]
                    else None,
                }
                for row in rows
            ]
    finally:
        conn.close()

    return {"status": "success", "count": len(data), "data": data}


@mcp.tool(
    description=(
        f"Validates {EMAIL_COUNT} emails via the agentesapi.27x.ai validation endpoint. "
        "Submits all emails, collects tracking IDs, waits 1 minute before the first poll, "
        "then polls all remaining emails per pass. After each pass that still has unresolved "
        "emails a 1 minute wait is applied before the next retry. Max 5 outer retry attempts "
        "per email. Returns scores and scoreStatuses (does NOT save to DB)."
    )
)
async def validate_mail(default: bool = True, mails: list = None) -> Dict[str, Any]:
    """Validate emails and return the results (without saving to DB).

    Flow:
      1. Submit all emails → collect tracking IDs.
      2. Wait 1 minute before the very first poll pass.
      3. Poll every remaining email once per outer pass.
      4. After each pass that still has unresolved emails, wait 1 minute
         before the next retry.
      5. Give up on an email after 5 failed outer attempts (records it as Timeout).
    """
    OUTER_MAX_ATTEMPTS = 5          # max outer retry passes per email
    INITIAL_WAIT_SECONDS = 120      # 2-min wait before first poll
    RETRY_WAIT_SECONDS   = 120      # 2-min wait between subsequent passes

    if default:
        emails = EMAILS_LIST
    else:
        if not mails:
            return {"error": "Give mails to validate"}
        emails = mails
    tracking_map: Dict[str, str] = {}

    async with httpx.AsyncClient() as client:
        print(f"[INFO] Submitting {len(emails)} emails for validation...")
        for email in emails:
            tracking_id = await submit_validation_request(email, client)
            if tracking_id:
                tracking_map[email] = tracking_id
            await asyncio.sleep(0.1)

        print(
            f"[INFO] Collected {len(tracking_map)} tracking IDs. "
            f"Waiting {INITIAL_WAIT_SECONDS}s before first poll pass..."
        )
        await asyncio.sleep(INITIAL_WAIT_SECONDS)

        results = []
        remaining = dict(tracking_map)
        outer_retry_counts: Dict[str, int] = {}
        pass_number = 0

        while remaining:
            pass_number += 1
            print(f"[INFO] Poll pass #{pass_number} — {len(remaining)} email(s) remaining...")
            next_remaining = {}

            for email, tracking_id in remaining.items():
                data = await poll_validation_result(tracking_id, client)
                if data:
                    results.append(
                        {
                            "email": data.get("email", email),
                            "score": data.get("score"),
                            "scoreStatus": data.get("scoreStatus"),
                            "overallStatus": data.get("overallStatus"),
                            "isValid": data.get("isValid"),
                        }
                    )
                else:
                    outer_retry_counts[email] = outer_retry_counts.get(email, 0) + 1
                    if outer_retry_counts[email] < OUTER_MAX_ATTEMPTS:
                        next_remaining[email] = tracking_id
                        print(
                            f"[INFO] {email} not resolved yet "
                            f"(attempt {outer_retry_counts[email]}/{OUTER_MAX_ATTEMPTS}) — will retry."
                        )
                    else:
                        print(
                            f"[WARN] Giving up on {email} after "
                            f"{OUTER_MAX_ATTEMPTS} outer attempts — recording as Timeout."
                        )
                        results.append(
                            {
                                "email": email,
                                "score": None,
                                "scoreStatus": "Pending",
                                "overallStatus": "Timeout",
                                "isValid": None,
                            }
                        )

            remaining = next_remaining
            if remaining:
                print(
                    f"[INFO] {len(remaining)} email(s) still pending — "
                    f"waiting {RETRY_WAIT_SECONDS}s before next pass..."
                )
                await asyncio.sleep(RETRY_WAIT_SECONDS)

    return {
        "status": "success",
        "submitted": len(tracking_map),
        "completed": len(results),
        "results": results,
    }


@mcp.tool(
    description=(
        "Finds the MX records for one or more given email addresses or domains using Google DNS. "
        "Accepts a list of emails or domain strings and returns the MX records for each."
    )
)
async def find_mx_record(emails_or_domains: List[str]) -> Dict[str, Any]:
    """Find MX records for the given email addresses or domain names.

    Args:
        emails_or_domains: A list of email addresses (e.g. ['user@example.com'])
                           or bare domain names (e.g. ['example.com', 'gmail.com']).
    """
    results = []
    for entry in emails_or_domains:
        entry = entry.strip()
        domain = extract_domain(entry) if "@" in entry else entry

        if not re.match(r"^[a-zA-Z0-9._-]+$", domain):
            results.append(
                {
                    "input": entry,
                    "domain": domain,
                    "error": "Invalid domain format",
                    "mx_records": [],
                    "mx_count": 0,
                }
            )
            continue

        answers = await asyncio.get_event_loop().run_in_executor(
            None, fetch_mx_records, domain
        )
        mx_entries = [
            {
                "name": answer.get("name"),
                "ttl": answer.get("TTL"),
                "data": answer.get("data"),
            }
            for answer in answers
        ]
        results.append(
            {
                "input": entry,
                "domain": domain,
                "mx_records": mx_entries,
                "mx_count": len(mx_entries),
            }
        )

    return {"status": "success", "queried": len(emails_or_domains), "results": results}


# ── BounceBan Helpers ──────────────────────────────────────────────────────────────
# CONFIRMED from live Postman calls (galactic-comet-480286 / BounceBan Developers):
#
# 1. Submit endpoint — GET https://api.bounceban.com/v1/verify/single
#    - Method: GET (the previous code incorrectly used POST).
#    - Email is passed as a QUERY PARAM (?email=...), NOT a JSON body.
#    - Optional query params: mode ("regular" default), url (webhook target).
#    - Auth header is the RAW key, no "Bearer " prefix:
#         Authorization: 1425c9dfad9ee0975167...
#    - Fast-path success response (verification finished synchronously):
#         {
#           "credits_consumed": 1, "credits_remaining": 99,
#           "email": "...", "id": "3zo-ab716f60",
#           "is_accept_all": false, "is_disposable": false,
#           "is_free": false, "is_role": true, "mode": "regular",
#           "mx_records": [...], "result": "deliverable", "score": 99,
#           "smtp_provider": "Google", "status": "success",
#           "verify_at": "2026-06-20T11:03:38.497Z"
#         }
#    - In-progress response (confirmed live, e.g. test@blackhole.webpagetest.org):
#         {
#           "id": "g8k-307a8545",
#           "msg": "The email verification process is not yet complete. Please
#                   DO NOT send the same requests repeatedly as each request
#                   will cost 1 credit. You can poll for the verification
#                   results using the .../v1/verify/single/status API with
#                   the provided id. Alternatively, you can receive the
#                   results via a webhook by specifying a target URL with
#                   the url parameter...",
#           "status": "verifying",
#           "try_again_at": 1781953806   # unix timestamp (seconds)
#         }
#
#    IMPORTANT: BounceBan's own response explicitly warns against re-calling
#    the submit endpoint to "retry" — doing so costs another credit every
#    time, regardless of whether verification already started. The previous
#    version of this code re-POSTed {"id": tracking_id} to the SUBMIT
#    endpoint as its "polling" mechanism, which was both the wrong endpoint
#    (that's not how the API works at all) and, had it used the right
#    endpoint shape, would still have burned credits unnecessarily.
#
# 2. Status endpoint — GET https://api.bounceban.com/v1/verify/single/status
#    - Polled with the "id" returned from the submit response — this is the
#      correct, credit-free way to wait for a "verifying" result to resolve.
#    - Expected to return the same final shape as the submit endpoint's
#      fast-path response once verification completes, or another
#      "verifying" + try_again_at payload if still in progress.
BOUNCEBAN_API_URL = "https://api.bounceban.com/v1/verify/single"
BOUNCEBAN_STATUS_URL = "https://api.bounceban.com/v1/verify/single/status"
BOUNCEBAN_API_KEY = "1425c9dfad9ee09751673156f32b54b3"

# Confirmed final statuses/results (seen live): status="success", result="deliverable".
# Other plausible terminal outcomes included defensively.
BOUNCEBAN_FINAL_STATUSES = {
    "success", "failed", "invalid", "error", "done",
    "deliverable", "undeliverable", "risky", "unknown",
}
# CONFIRMED live pending status string — must NOT be treated as final.
BOUNCEBAN_PENDING_STATUSES = {"verifying"}


def _bounceban_seconds_until(unix_timestamp: Optional[int], default: float = 3.0) -> float:
    """Seconds to wait until the given unix timestamp, clamped to [1, 30]."""
    if not unix_timestamp:
        return default
    delta = unix_timestamp - time.time()
    return max(1.0, min(delta, 30.0))


async def _bounceban_submit(
    email: str, client: httpx.AsyncClient
) -> tuple[Optional[str], Optional[Dict[str, Any]], Optional[int]]:
    """Submit a single email to BounceBan via GET ?email=... (confirmed contract).

    Returns (tracking_id, immediate_result, try_again_at):
      • If the response already contains a final status/result, tracking_id
        is None and immediate_result holds the full JSON.
      • If the response status is "verifying", tracking_id is the
        verification's "id" and try_again_at is the unix timestamp at which
        BounceBan says it's safe to check the STATUS endpoint — the caller
        must poll /v1/verify/single/status with this id, and must NOT
        re-call this submit endpoint again (each such call costs a credit).
    """
    import os
    api_key = BOUNCEBAN_API_KEY or os.environ.get("BOUNCEBAN_API_KEY", "")
    headers = {"Authorization": api_key}  # raw key, NOT "Bearer {key}"
    try:
        response = await client.get(
            BOUNCEBAN_API_URL,
            params={"email": email},
            headers=headers,
            timeout=90.0,
        )
        if response.status_code == 200:
            data = response.json()
            status = str(data.get("status", "")).lower()
            result = str(data.get("result", "")).lower()

            # If the result is already in a final state, return it immediately
            if status in BOUNCEBAN_FINAL_STATUSES or result in BOUNCEBAN_FINAL_STATUSES:
                return None, data, None

            # "verifying" — still in progress; grab id + try_again_at for status polling
            if status in BOUNCEBAN_PENDING_STATUSES:
                tracking_id = data.get("id")
                try_again_at = data.get("try_again_at")
                print(
                    f"[INFO] BounceBan: {email} → id={tracking_id}, status='verifying' — "
                    f"will poll status endpoint (try_again_at={try_again_at})."
                )
                return tracking_id, None, try_again_at

            # Unrecognized shape — surface it rather than silently dropping it.
            print(f"[WARN] BounceBan: {email} returned unrecognized shape: {data}")
            return None, data, None
        else:
            print(
                f"[WARN] BounceBan submit failed for {email}: "
                f"{response.status_code} - {response.text}"
            )
            return None, {
                "email": email,
                "status": "error",
                "result": "submission_failed",
                "http_status": response.status_code,
                "error_detail": response.text[:500],
            }, None
    except Exception as e:
        print(f"[ERROR] BounceBan submit exception for {email}: {e}")
        return None, {
            "email": email,
            "status": "error",
            "result": "submission_failed",
            "error_detail": str(e),
        }, None


async def _bounceban_poll(
    tracking_id: str,
    client: httpx.AsyncClient,
    try_again_at: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    """Poll BounceBan's dedicated status endpoint with the verification id
    until a final status/result is returned.

    Respects the server-provided `try_again_at` unix timestamp for pacing
    instead of a fixed/guessed delay, falling back to a 3s default if it's
    missing on a later iteration.

    This is the correct, credit-free way to wait for a "verifying" result —
    it replaces the previous (incorrect) approach of re-POSTing
    {"id": tracking_id} to the submit endpoint, which doesn't match
    BounceBan's actual API shape and, per BounceBan's own messaging, would
    have cost a fresh credit on every attempt regardless.
    """
    import os
    api_key = BOUNCEBAN_API_KEY or os.environ.get("BOUNCEBAN_API_KEY", "")
    headers = {"Authorization": api_key}
    max_attempts = 200  # try_again_at paces the real wait; this is just a safety ceiling

    for attempt in range(max_attempts):
        await asyncio.sleep(_bounceban_seconds_until(try_again_at))

        try:
            response = await client.get(
                BOUNCEBAN_STATUS_URL,
                params={"id": tracking_id},
                headers=headers,
                timeout=30.0,
            )
            if response.status_code == 200:
                data = response.json()
                status = str(data.get("status", "")).lower()
                result = str(data.get("result", "")).lower()

                if status in BOUNCEBAN_FINAL_STATUSES or result in BOUNCEBAN_FINAL_STATUSES:
                    return data

                if status in BOUNCEBAN_PENDING_STATUSES:
                    try_again_at = data.get("try_again_at")
                    print(
                        f"[INFO] BounceBan poll {attempt + 1}: id={tracking_id} "
                        f"still 'verifying' — next attempt per try_again_at={try_again_at}"
                    )
                else:
                    print(
                        f"[WARN] BounceBan poll {attempt + 1}: id={tracking_id} "
                        f"unrecognized status={status!r} result={result!r} — continuing to poll."
                    )
                    try_again_at = None
            elif response.status_code == 404:
                print(f"[ERROR] BounceBan: id={tracking_id} not found (404) — stopping poll.")
                return {"status": "error", "result": "tracking_id_not_found", "id": tracking_id}
            else:
                print(
                    f"[WARN] BounceBan poll error for id={tracking_id}: "
                    f"{response.status_code} - {response.text[:300]}"
                )
                try_again_at = None
        except Exception as e:
            print(f"[ERROR] BounceBan poll exception (id={tracking_id}): {e}")
            try_again_at = None

    print(f"[WARN] BounceBan: max polling attempts reached for id={tracking_id}")
    return None


# ── MCP Tool: verify_in_bounceban ─────────────────────────────────────────────────

@mcp.tool(
    description=(
        "Validates a list of email addresses using the BounceBan single-verification "
        "API (https://api.bounceban.com/v1/verify/single). For each email, submits a "
        "GET request with the email as a query parameter. If verification completes "
        "synchronously, returns the result immediately. If BounceBan returns a "
        "'verifying' status with a tracking id, polls the dedicated status endpoint "
        "(/v1/verify/single/status) — paced by the server-provided try_again_at "
        "timestamp — until a final result is reached. Never re-calls the submit "
        "endpoint to retry, since each such call costs an additional credit. Returns "
        "the full raw JSON response from BounceBan for each email."
    )
)
async def verify_in_bounceban(emails: List[str]) -> Dict[str, Any]:
    """Verify email addresses via the BounceBan single-verify API.

    Args:
        emails: A list of email addresses to verify, e.g.
                ['dev@bounceban.com', 'user@example.com'].

    Returns:
        A dict with 'status', 'submitted', 'completed', and 'results' (list of raw
        BounceBan JSON objects, one per email, in original input order).
    """
    resolved: Dict[str, Dict[str, Any]] = {}
    # email -> (tracking_id, try_again_at) for emails still verifying
    pending: Dict[str, tuple] = {}

    async with httpx.AsyncClient() as client:
        # ── Step 1: Submit all emails ─────────────────────────────────────────
        print(f"[INFO] BounceBan: submitting {len(emails)} emails...")
        for email in emails:
            email = email.strip()
            tracking_id, immediate_result, try_again_at = await _bounceban_submit(email, client)
            if immediate_result is not None:
                resolved[email] = immediate_result
                print(
                    f"[INFO] BounceBan: {email} resolved immediately → "
                    f"{immediate_result.get('result')}"
                )
            elif tracking_id is not None:
                pending[email] = (tracking_id, try_again_at)
            else:
                # Submission failed entirely
                resolved[email] = {
                    "email": email,
                    "status": "error",
                    "result": "submission_failed",
                }
            await asyncio.sleep(0.2)  # gentle rate-limiting between submissions

        print(
            f"[INFO] BounceBan: {len(resolved)} resolved immediately, "
            f"{len(pending)} need status polling."
        )

        # ── Step 2: Poll the STATUS endpoint for each pending email ──────────
        # (No outer retry loop needed here — _bounceban_poll already loops
        # internally, paced by try_again_at, until a final result or a
        # generous safety ceiling is reached.)
        for email, (tracking_id, try_again_at) in pending.items():
            data = await _bounceban_poll(tracking_id, client, try_again_at)
            if data is not None:
                resolved[email] = data
                print(
                    f"[INFO] BounceBan: {email} resolved after polling → "
                    f"{data.get('result')}"
                )
            else:
                resolved[email] = {
                    "email": email,
                    "status": "timeout",
                    "result": "unresolved",
                    "id": tracking_id,
                }

    # ── Step 3: Assemble results in original email order ─────────────────────
    results = [
        resolved.get(email.strip(), {"email": email.strip(), "status": "error", "result": "unknown"})
        for email in emails
    ]

    return {
        "status": "success",
        "submitted": len(emails),
        "completed": len(results),
        "results": results,
    }


# ── ZeroBounce Helpers ─────────────────────────────────────────────────────────────
ZEROBOUNCE_API_URL = "https://api.zerobounce.net/v2/validate"
ZEROBOUNCE_API_KEY = "b3213412711145258e9b7f9b96a3654c"  

# ZeroBounce statuses that are considered fully resolved (no further polling needed)
ZEROBOUNCE_FINAL_STATUSES = {
    "valid", "invalid", "catch-all", "unknown", "spamtrap",
    "abuse", "do_not_mail", "error",
}


async def _zerobounce_validate(
    email: str, client: httpx.AsyncClient
) -> Optional[Dict[str, Any]]:
    """Call the ZeroBounce v2/validate endpoint for a single email.

    Returns the full JSON dict when the response contains a recognised final
    status, or None when the result is not yet available / the request failed.
    """
    import os
    api_key = ZEROBOUNCE_API_KEY or os.environ.get("ZEROBOUNCE_API_KEY", "")
    params = {
        "api_key": api_key,
        "email": email,
        "ip_address": "",          # optional; leave blank for basic validation
    }
    try:
        response = await client.get(
            ZEROBOUNCE_API_URL,
            params=params,
            timeout=30.0,
        )
        if response.status_code == 200:
            data = response.json()
            status = str(data.get("status", "")).lower()

            if status in ZEROBOUNCE_FINAL_STATUSES:
                return data

            # Status present but not a recognised final value — treat as pending
            if status:
                print(
                    f"[INFO] ZeroBounce: {email} returned status='{status}' "
                    "(not final) — will retry."
                )
            else:
                print(f"[INFO] ZeroBounce: {email} returned no status yet — will retry.")

            return None
        else:
            print(
                f"[WARN] ZeroBounce: {email} HTTP {response.status_code} — "
                f"{response.text[:200]}"
            )
            return None
    except Exception as e:
        print(f"[ERROR] ZeroBounce: exception validating {email}: {e}")
        return None


# ── MCP Tool: verify_in_zerobounce ────────────────────────────────────────────────

@mcp.tool(
    description=(
        "Validates a list of email addresses using the ZeroBounce v2 API "
        "(https://api.zerobounce.net/v2/validate). "
        "For each email the tool calls the validation endpoint. If the result is "
        "not yet in a final state the email is kept in a pending set and retried "
        "after a short interval. The outer while-loop continues until every email "
        "in the list has been resolved or has exhausted 5 retry attempts. "
        "Returns the full raw JSON response from ZeroBounce for each email."
    )
)
async def verify_in_zerobounce(emails: List[str]) -> Dict[str, Any]:
    """Verify email addresses via the ZeroBounce v2/validate API.

    Args:
        emails: A list of email addresses to verify, e.g.
                ['john.doe@zerobounce.net', 'user@example.com'].

    Returns:
        A dict with 'status', 'submitted', 'completed', and 'results' (list of raw
        ZeroBounce JSON objects, one per email, in the original input order).
    """
    OUTER_MAX_ATTEMPTS  = 5    # max retry passes per email before giving up
    RETRY_WAIT_SECONDS  = 30   # seconds between outer retry passes

    resolved: Dict[str, Dict[str, Any]] = {}
    outer_retry_counts: Dict[str, int] = {}

    # Normalise input once
    email_list = [e.strip() for e in emails]

    # All emails start as pending
    pending: List[str] = list(email_list)

    async with httpx.AsyncClient() as client:
        pass_number = 0

        while pending:
            pass_number += 1
            print(
                f"[INFO] ZeroBounce: pass #{pass_number} — "
                f"validating {len(pending)} email(s)..."
            )
            next_pending: List[str] = []

            for email in pending:
                data = await _zerobounce_validate(email, client)

                if data is not None:
                    resolved[email] = data
                    print(
                        f"[INFO] ZeroBounce: {email} resolved → "
                        f"status={data.get('status')}"
                    )
                else:
                    outer_retry_counts[email] = outer_retry_counts.get(email, 0) + 1
                    if outer_retry_counts[email] < OUTER_MAX_ATTEMPTS:
                        next_pending.append(email)
                        print(
                            f"[INFO] ZeroBounce: {email} not resolved "
                            f"(attempt {outer_retry_counts[email]}/{OUTER_MAX_ATTEMPTS}) "
                            "— will retry."
                        )
                    else:
                        print(
                            f"[WARN] ZeroBounce: giving up on {email} after "
                            f"{OUTER_MAX_ATTEMPTS} attempts."
                        )
                        resolved[email] = {
                            "address": email,
                            "status": "timeout",
                            "sub_status": "unresolved_after_retries",
                        }

                # Small courtesy delay between individual requests
                await asyncio.sleep(0.3)

            pending = next_pending
            if pending:
                print(
                    f"[INFO] ZeroBounce: {len(pending)} email(s) still pending — "
                    f"waiting {RETRY_WAIT_SECONDS}s before next pass..."
                )
                await asyncio.sleep(RETRY_WAIT_SECONDS)

    # Assemble results in original email order
    results = [
        resolved.get(email, {"address": email, "status": "error", "sub_status": "unknown"})
        for email in email_list
    ]

    return {
        "status": "success",
        "submitted": len(email_list),
        "completed": len(results),
        "results": results,
    }


# ── Entry Point ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    mcp.run(transport="stdio")
