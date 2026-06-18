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

# ── Load Environment Configuration ──────────────────────────────────────────────
current_dir = Path(__file__).resolve().parent
load_dotenv(dotenv_path=current_dir / ".env")
load_dotenv(dotenv_path=current_dir.parent / ".env")

# ── Constants ────────────────────────────────────────────────────────────────────
VALIDATE_EMAIL_URL = "https://agentesapi.27x.ai/validate-email"
VALIDATE_EMAIL_RESULT_URL = "https://agentesapi.27x.ai/validate-email/result"
DB_URL = "postgresql://n2b_user:VI8k3lf9JcU9otl2O8Rq736Vraug1rko@dpg-d8pnk4jtqb8s738c261g-a.oregon-postgres.render.com/n2b"

# 73 sample email addresses to validate
EMAILS_LIST: List[str] = [
    "pkim@ioufinancial.com","edwin.vargas@roadsync.com","bruce.greeson@roadsync.com","ward.howell@withclutch.com"
    "adam.adam@moniepoint.com","nitu.kalyani@pw.live","ramya.ghulati@pw.live","devashree.bartaria@pw.live",
    "travis.eldik@frazierdeeter.com","jerryy@prmg.net","anorman@guildmortgage.net","sharon.w@elementfunding.com",
    "scottw@cnanational.com","katherineh@daveramsey.com","cline@cuofga.org","agbontaen.ann@banksouthmortgage.com",
    "brownerica@firstcommand.com","asensookyere@unitedfidelity.com","blake.adams@frazierdeeter.com",
    "keoni.liang@andersen.com","evan.troutt@frazierdeeter.com","maurice.nichols@fifsg.com",
    "russell.peggues@fifsg.com","dblankenship@renasant.com",
    "anastasia.jordan@lendmarkfinancial.com","kelsea.white@lbmc.com","ashley.shaffer@frazierdeeter.com",
    "elijah.briscoe@frazierdeeter.com","martin.magee@mcaleer-rushe.co.uk",
    "bruna.arcibelli@fhb.com","jake.hijirida@fhb.com","anthony.wong@fhb.com","joanna.liu@fhb.com",
    "nicholas.bottom@fhb.com","ben.kashiwabara@fhb.com","craymond@fhb.com","davidson@fhb.com",
    "gary.yu@fhb.com","chev.kodama@fhb.com","bernadette.andrews@amerisbank.com","jeffrey.higashi@fhb.com",
    "brandon.aurelio@fhb.com","alistair.cameron@fhb.com","doc_h_elcc@yahoo.com","debbieinks04@yahoo.com",
    "jami_colorado79@yahoo.com","lynnguyen303@yahoo.com","dgelles@yahoo.com","jake_skow@yahoo.com",
    "lomeliw@yahoo.com","n640@yahoo.com","ejones1434@yahoo.com","gbearly@yahoo.com",
    "dziroli@yahoo.com","devon.hopkins@yahoo.com","lazyrmei999@yahoo.com","fshokouhi@yahoo.com",
    "hkhoshnevisan@yahoo.com","bzmom45@yahoo.com","szeivaz@yahoo.com","stevechaijr@yahoo.com",
    "domlam28@yahoo.com","alan.wolfer@yahoo.com","chi_to@yahoo.com","chi.to@yahoo.com",
    "ericallegakoen@yahoo.com","aweemaes@yahoo.com","whalen_amber@yahoo.com",
]   

# ── Initialize FastMCP Server ────────────────────────────────────────────────────
mcp = FastMCP(name="N2B Utils", version="1.0.0")


# ── Database Helpers ─────────────────────────────────────────────────────────────

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
        cur.execute("ALTER TABLE email_validations ADD COLUMN IF NOT EXISTS mx_records JSONB;")
    conn.commit()


def upsert_email_validation(conn, email: str, score=None, score_status: str = None,
                             overall_status: str = None, is_valid: bool = None) -> None:
    """Upsert validation results for a single email into the DB."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO email_validations (email, score, score_status, overall_status, is_valid, updated_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (email) DO UPDATE SET
                score = COALESCE(EXCLUDED.score, email_validations.score),
                score_status = COALESCE(EXCLUDED.score_status, email_validations.score_status),
                overall_status = COALESCE(EXCLUDED.overall_status, email_validations.overall_status),
                is_valid = COALESCE(EXCLUDED.is_valid, email_validations.is_valid),
                updated_at = NOW()
        """, (email, score, score_status, overall_status, is_valid))
    conn.commit()


def upsert_mx_records(conn, email: str, mx_count: int, mx_records: List[Dict[str, Any]]) -> None:
    """Upsert MX record count and records for a single email into the DB."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO email_validations (email, mx_record_count, mx_records, updated_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (email) DO UPDATE SET
                mx_record_count = EXCLUDED.mx_record_count,
                mx_records = EXCLUDED.mx_records,
                updated_at = NOW()
        """, (email, mx_count, json.dumps(mx_records)))
    conn.commit()


# ── Email Validation Helpers ─────────────────────────────────────────────────────

async def submit_validation_request(email: str, client: httpx.AsyncClient) -> Optional[str]:
    """Submit an email for validation and return the tracking ID."""
    try:
        response = await client.post(VALIDATE_EMAIL_URL, json={"email": email}, timeout=30.0)
        if response.status_code == 200:
            data = response.json()
            # The API may return trackingId directly or nested
            tracking_id = data.get("trackingId") or data.get("tracking_id") or data.get("id")
            return tracking_id
        else:
            print(f"[WARN] Failed to submit {email}: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"[ERROR] Exception submitting {email}: {e}")
        return None


async def poll_validation_result(tracking_id: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    """Poll with exponential backoff to avoid rate limits and infinite hangs."""
    final_statuses = {"completed", "failed", "invalid", "valid", "error", "done"}
    max_attempts = 15     # A reasonable cap (will take about 2-3 minutes total with backoff)
    current_delay = 2.0   # Start with a 2-second delay
    max_delay = 15.0      # Never wait more than 15 seconds between pings

    for attempt in range(max_attempts):
        try:
            response = await client.get(
                VALIDATE_EMAIL_RESULT_URL,
                params={"trackingId": tracking_id},
                timeout=15.0, # 60s is too long for a single ping. Fail fast, try again.
                follow_redirects=True,
            )
            
            if response.status_code == 200:
                data = response.json()
                overall_status = str(data.get("overallStatus", "")).lower()
                status = str(data.get("status", "")).lower()

                # Success conditions
                if overall_status in final_statuses or status in final_statuses:
                    return data
                if data.get("email") and data.get("score") is not None:
                    return data
                    
            elif response.status_code == 429:
                print(f"[WARN] Rate limited (429) on {tracking_id}. Backing off...")
                # We don't break; we just let the delay increase below
                
            elif response.status_code >= 400 and response.status_code != 429:
                # If it's a 404 or 500, logging it helps debug why emails drop
                print(f"[ERROR] API returned {response.status_code} for {tracking_id}")

            # Wait, then increase the delay for the next loop (Exponential Backoff)
            await asyncio.sleep(current_delay)
            current_delay = min(current_delay * 1.5, max_delay)

        except Exception as e:
            print(f"[ERROR] Polling {tracking_id} attempt {attempt + 1}: {e}")
            await asyncio.sleep(current_delay)
            current_delay = min(current_delay * 1.5, max_delay)

    print(f"[WARN] Gave up on {tracking_id} after {max_attempts} attempts.")
    return None


# ── MX Record Helpers ────────────────────────────────────────────────────────────

def extract_domain(email: str) -> str:
    """Extract the domain from an email address."""
    parts = email.split("@")
    return parts[-1] if len(parts) == 2 else email


def fetch_mx_records(domain: str) -> List[Dict[str, Any]]:
    """Fetch MX records for a domain using Google DNS over HTTPS."""
    # Validate domain to prevent shell injection
    if not re.match(r"^[a-zA-Z0-9._-]+$", domain):
        return []

    try:
        result = subprocess.run(
            ["curl", "-s", f"https://dns.google/resolve?name={domain}&type=MX"],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode != 0:
            return []

        data = json.loads(result.stdout)
        answers = data.get("Answer", [])
        return answers if isinstance(answers, list) else []
    except Exception as e:
        print(f"[ERROR] MX lookup for {domain}: {e}")
        return []


# ── MCP Tool Implementations ─────────────────────────────────────────────────────

@mcp.tool(description=(
    "Validates 73 emails via the agentesapi.27x.ai validation endpoint. "
    "Submits all emails, collects tracking IDs, polls until all reach a final state, "
    "then saves the score and scoreStatus of each email into PostgreSQL."
))
async def validate_mail_save() -> Dict[str, Any]:
    """Validate 73 emails and persist results to the PostgreSQL database."""
    emails = EMAILS_LIST[:73]
    tracking_map: Dict[str, str] = {}  # email -> trackingId

    async with httpx.AsyncClient() as client:
        # Step 1: Submit all emails and collect tracking IDs
        print(f"[INFO] Submitting {len(emails)} emails for validation...")
        for email in emails:
            tracking_id = await submit_validation_request(email, client)
            if tracking_id:
                tracking_map[email] = tracking_id
            await asyncio.sleep(0.1)  # slight rate-limit guard

        print(f"[INFO] Collected {len(tracking_map)} tracking IDs. Polling for results...")

        # Step 2: Poll results until all reach final state
        results = []
        remaining = dict(tracking_map)  # email -> trackingId still pending

        while remaining:
            next_remaining = {}
            for email, tracking_id in remaining.items():
                data = await poll_validation_result(tracking_id, client)
                if data:
                    results.append({
                        "email": data.get("email", email),
                        "score": data.get("score"),
                        "scoreStatus": data.get("scoreStatus"),
                        "overallStatus": data.get("overallStatus"),
                        "isValid": data.get("isValid"),
                    })
                else:
                    next_remaining[email] = tracking_id
            remaining = next_remaining
            if remaining:
                print(f"[INFO] {len(remaining)} still pending, retrying...")
                await asyncio.sleep(3)

    # Step 3: Persist results to PostgreSQL
    print(f"[INFO] Saving {len(results)} results to database...")
    conn = get_db_connection()
    try:
        ensure_email_table(conn)
        for row in results:
            upsert_email_validation(
                conn,
                email=row["email"],
                score=row["score"],
                score_status=row["scoreStatus"],
                overall_status=row["overallStatus"],
                is_valid=row["isValid"],
            )
    finally:
        conn.close()

    return {
        "status": "success",
        "submitted": len(tracking_map),
        "saved": len(results),
        "results": results,
    }


@mcp.tool(description=(
    "Looks up MX records for the domains of 73 emails using Google DNS (dns.google). "
    "Stores the computed MX records and their count for each email in PostgreSQL."
))
async def mx_record_save() -> Dict[str, Any]:
    """Find MX records for 73 email domains and save the records to PostgreSQL."""
    emails = EMAILS_LIST[:73]
    mx_results = []

    print(f"[INFO] Fetching MX records for {len(emails)} emails...")
    for email in emails:
        domain = extract_domain(email)
        answers = await asyncio.get_event_loop().run_in_executor(None, fetch_mx_records, domain)
        mx_entries = []
        for answer in answers:
            mx_entries.append({
                "name": answer.get("name"),
                "ttl": answer.get("TTL"),
                "data": answer.get("data"),
            })
        mx_count = len(mx_entries)
        mx_results.append({"email": email, "domain": domain, "mx_count": mx_count, "mx_records": mx_entries})

    # Save to PostgreSQL
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


@mcp.tool(description=(
    "Fetches the 73 emails along with their validation scores, score statuses, "
    "MX records, and other fields from the PostgreSQL database."
))
async def get_data() -> Dict[str, Any]:
    """Retrieve email validation and MX record data from the PostgreSQL database."""
    conn = get_db_connection()
    try:
        ensure_email_table(conn)
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT email, score, score_status, overall_status, is_valid, mx_record_count, mx_records, updated_at
                FROM email_validations
                ORDER BY id
                LIMIT 73
            """)
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
                    "updatedAt": row["updated_at"].isoformat() if row["updated_at"] else None,
                }
                for row in rows
            ]
    finally:
        conn.close()

    return {
        "status": "success",
        "count": len(data),
        "data": data,
    }


@mcp.tool(description=(
    "Validates 73 emails via the agentesapi.27x.ai validation endpoint. "
    "Submits all emails, collects tracking IDs, polls until all reach a final state, "
    "and returns the email addresses with their scores and scoreStatuses (does NOT save to DB)."
))
async def validate_mail() -> Dict[str, Any]:
    """Validate 73 emails and return the results (without saving to DB)."""
    emails = EMAILS_LIST[:73]
    tracking_map: Dict[str, str] = {}  # email -> trackingId

    async with httpx.AsyncClient() as client:
        # Step 1: Submit all emails and collect tracking IDs
        print(f"[INFO] Submitting {len(emails)} emails for validation...")
        for email in emails:
            tracking_id = await submit_validation_request(email, client)
            if tracking_id:
                tracking_map[email] = tracking_id
            await asyncio.sleep(0.1)

        print(f"[INFO] Collected {len(tracking_map)} tracking IDs. Polling for results...")

        # Step 2: Poll results until all reach final state
        results = []
        remaining = dict(tracking_map)

        while remaining:
            next_remaining = {}
            for email, tracking_id in remaining.items():
                data = await poll_validation_result(tracking_id, client)
                if data:
                    results.append({
                        "email": data.get("email", email),
                        "score": data.get("score"),
                        "scoreStatus": data.get("scoreStatus"),
                        "overallStatus": data.get("overallStatus"),
                        "isValid": data.get("isValid"),
                    })
                else:
                    next_remaining[email] = tracking_id
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


@mcp.tool(description=(
    "Finds the MX records for one or more given email addresses or domains using Google DNS. "
    "Accepts a list of emails or domain strings and returns the MX records for each."
))
async def find_mx_record(emails_or_domains: List[str]) -> Dict[str, Any]:
    """
    Find MX records for the given email addresses or domain names.

    Args:
        emails_or_domains: A list of email addresses (e.g. ['user@example.com'])
                           or bare domain names (e.g. ['example.com', 'gmail.com']).
    """
    results = []

    for entry in emails_or_domains:
        entry = entry.strip()
        # If it looks like an email, extract the domain
        domain = extract_domain(entry) if "@" in entry else entry

        if not re.match(r"^[a-zA-Z0-9._-]+$", domain):
            results.append({
                "input": entry,
                "domain": domain,
                "error": "Invalid domain format",
                "mx_records": [],
                "mx_count": 0,
            })
            continue

        answers = await asyncio.get_event_loop().run_in_executor(None, fetch_mx_records, domain)
        mx_entries = []
        for answer in answers:
            mx_entries.append({
                "name": answer.get("name"),
                "ttl": answer.get("TTL"),
                "data": answer.get("data"),
            })

        results.append({
            "input": entry,
            "domain": domain,
            "mx_records": mx_entries,
            "mx_count": len(mx_entries),
        })

    return {
        "status": "success",
        "queried": len(emails_or_domains),
        "results": results,
    }


# ── Entry Point ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    mcp.run(transport="stdio")
