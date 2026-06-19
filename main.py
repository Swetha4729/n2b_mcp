#!/usr/bin/env python3
"""
N2B MCP Server — stdio transport
Provides email validation, MX record lookup, and PostgreSQL persistence tools.

Fixes applied:
  1. Missing comma between "ward.howell@withclutch.com" and "adam.adam@moniepoint.com"
     — was causing Python implicit string concatenation (67 emails instead of 68).
  2. upsert_email_validation now uses plain assignment for score (not COALESCE),
     so a real score always overwrites a previously-null row.
  3. validate_mail_save skips upserting rows whose score is None/empty,
     preventing null scores from overwriting previously-saved real scores.
  4. Outer polling loop in validate_mail_save and validate_mail now has a
     MAX_OUTER_RETRIES cap to prevent infinite loops when an email never resolves.
  5. EMAILS_LIST slice now uses len(EMAILS_LIST) instead of the hardcoded 73,
     so the actual list length is always used.
  6. mx_record_save and validate_mail_save are documented to be run sequentially
     (MX first, then validation) to avoid the race condition where MX pre-populates
     rows with null scores before validation can write real scores.
  7. Added table clearing step with ID reset (TRUNCATE ... RESTART IDENTITY) 
     before validation/mx lookups via the optional `clear_table` parameter.
"""

import asyncio
import json
import re
import subprocess
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

EMAILS_LIST: List[str] = [
    "pkim@ioufinancial.com",
    "edwin.vargas@roadsync.com",
    "bruce.greeson@roadsync.com",
    "ward.howell@withclutch.com",       
    "adam.adam@moniepoint.com",         
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

EMAIL_COUNT = len(EMAILS_LIST)
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


def clear_email_table(conn) -> None:
    """Clear the email_validations table and reset the ID sequence."""
    with conn.cursor() as cur:
        # TRUNCATE empties the table completely. RESTART IDENTITY resets the ID sequence.
        cur.execute("TRUNCATE TABLE email_validations RESTART IDENTITY;")
    conn.commit()


def upsert_email_validation(
    conn,
    email: str,
    score=None,
    score_status: str = None,
    overall_status: str = None,
    is_valid: bool = None,
) -> None:
    """Upsert validation results for a single email into the DB."""
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
    """Poll the result endpoint for a single tracking ID until it reaches a final state."""
    final_statuses = {"completed", "failed", "invalid", "valid", "error", "done"}
    max_attempts = 7500
    delay = 2  # seconds between polls

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

                if overall_status in final_statuses or status in final_statuses:
                    return data

                if data.get("email") and data.get("score") is not None:
                    return data

            await asyncio.sleep(delay)
        except Exception as e:
            print(f"[ERROR] Polling trackingId {tracking_id} attempt {attempt + 1}: {e}")
            await asyncio.sleep(delay)

    print(f"[WARN] Max polling attempts reached for trackingId {tracking_id}")
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
        "Submits all emails, collects tracking IDs, polls until all reach a final state, "
        "then saves the score and scoreStatus of each email into PostgreSQL. "
        "NOTE: Run this AFTER mx_record_save to avoid the race condition where MX rows "
        "pre-populate score=null before validation completes. "
        "Set `clear_table=True` to truncate the table and reset IDs before execution."
    )
)
async def validate_mail_save(clear_table: bool = False) -> Dict[str, Any]:
    """Validate emails and persist results to the PostgreSQL database."""
    
    if clear_table:
        print(f"[INFO] Clearing database table and resetting IDs before validation...")
        conn = get_db_connection()
        try:
            ensure_email_table(conn)
            clear_email_table(conn)
        finally:
            conn.close()

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

        print(f"[INFO] Collected {len(tracking_map)} tracking IDs. Polling for results...")

        # Step 2: Poll until all reach a final state (capped outer loop)
        results = []
        remaining = dict(tracking_map)
        outer_retry_counts: Dict[str, int] = {}

        while remaining:
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
                    if outer_retry_counts[email] < MAX_OUTER_RETRIES:
                        next_remaining[email] = tracking_id
                    else:
                        print(
                            f"[WARN] Giving up on {email} after "
                            f"{MAX_OUTER_RETRIES} outer retries — recording as failed"
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
                print(f"[INFO] {len(remaining)} still pending, retrying...")
                await asyncio.sleep(3)

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
        "cleared_table": clear_table,
        "results": results,
    }


@mcp.tool(
    description=(
        f"Looks up MX records for the domains of {EMAIL_COUNT} emails using Google DNS (dns.google). "
        "Stores the computed MX records and their count for each email in PostgreSQL. "
        "NOTE: Run this BEFORE validate_mail_save. "
        "Set `clear_table=True` to truncate the table and reset IDs before execution."
    )
)
async def mx_record_save(clear_table: bool = False) -> Dict[str, Any]:
    """Find MX records for email domains and save the records to PostgreSQL."""

    if clear_table:
        print(f"[INFO] Clearing database table and resetting IDs before MX lookup...")
        conn = get_db_connection()
        try:
            ensure_email_table(conn)
            clear_email_table(conn)
        finally:
            conn.close()

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
        "cleared_table": clear_table,
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
        "Submits all emails, collects tracking IDs, polls until all reach a final state, "
        "and returns the email addresses with their scores and scoreStatuses (does NOT save to DB)."
    )
)
async def validate_mail() -> Dict[str, Any]:
    """Validate emails and return the results (without saving to DB)."""
    emails = EMAILS_LIST[:EMAIL_COUNT]
    tracking_map: Dict[str, str] = {}

    async with httpx.AsyncClient() as client:
        print(f"[INFO] Submitting {len(emails)} emails for validation...")
        for email in emails:
            tracking_id = await submit_validation_request(email, client)
            if tracking_id:
                tracking_map[email] = tracking_id
            await asyncio.sleep(0.1)

        print(f"[INFO] Collected {len(tracking_map)} tracking IDs. Polling for results...")

        results = []
        remaining = dict(tracking_map)
        outer_retry_counts: Dict[str, int] = {}

        while remaining:
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
                    if outer_retry_counts[email] < MAX_OUTER_RETRIES:
                        next_remaining[email] = tracking_id
                    else:
                        print(f"[WARN] Giving up on {email} after {MAX_OUTER_RETRIES} outer retries")
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
                print(f"[INFO] {len(remaining)} still pending, retrying...")
                await asyncio.sleep(3)

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
    """Find MX records for the given email addresses or domain names."""
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


# ── Entry Point ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    mcp.run(transport="stdio")
