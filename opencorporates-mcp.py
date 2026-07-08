#!/usr/bin/env python3
"""
OpenCorporates MCP Server
Provides tools for interacting with the OpenCorporates API.
"""

import os
from typing import Any, Dict, Optional
import httpx
from fastmcp import FastMCP
from dotenv import load_dotenv

# Load Environment Configuration
load_dotenv()

# Initialize FastMCP Server
mcp = FastMCP(name="OpenCorporates Utils", version="1.0.0")

BASE_URL = "https://api.opencorporates.com"

def get_base_params() -> Dict[str, Any]:
    """Get base parameters including API token if available."""
    params = {}
    api_token = os.environ.get("OPENCORPORATES_API_TOKEN")
    if api_token:
        params["api_token"] = api_token
    return params

@mcp.tool(
    description=(
        "Discover companies globally or within a specific jurisdiction based on a search term. "
        "This is the entry point for most corporate investigations when the exact company number is unknown."
    )
)
async def search_companies(
    jurisdiction_code: str,
    company_number: str,
    q: Optional[str] = None,
    sparse: bool = False
) -> Dict[str, Any]:
    """Search for companies."""
    params = get_base_params()
    if q:
        params["q"] = q
    params["jurisdiction_code"] = jurisdiction_code
    params["company_number"] = company_number
    if sparse:
        params["sparse"] = "true"
        
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(f"{BASE_URL}/companies/search", params=params, timeout=30.0)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            return {"error": str(e)}

@mcp.tool(
    description=(
        "Retrieves deep, comprehensive data about a specific corporate entity. "
        "This includes core data (incorporation date, status, addresses), parent company links, and recent statutory filings."
    )
)
async def get_company_details(
    jurisdiction_code: str,
    company_number: str,
    sparse: bool = False
) -> Dict[str, Any]:
    """Get company details."""
    params = get_base_params()
    if sparse:
        params["sparse"] = "true"
        
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(f"{BASE_URL}/companies/{jurisdiction_code}/{company_number}", params=params, timeout=30.0)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            return {"error": str(e)}

@mcp.tool(
    description=(
        "Search for individual directors, secretaries, or corporate officers by name across the globe. "
        "This is vital for mapping human networks and uncovering conflicts of interest."
    )
)
async def search_officers(
    q: str,
    jurisdiction_code: Optional[str] = None,
    position: Optional[str] = None
) -> Dict[str, Any]:
    """Search for officers."""
    params = get_base_params()
    params["q"] = q
    if jurisdiction_code:
        params["jurisdiction_code"] = jurisdiction_code
    if position:
        params["position"] = position
        
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(f"{BASE_URL}/officers/search", params=params, timeout=30.0)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            return {"error": str(e)}

@mcp.tool(
    description=(
        "Retrieves the chronological history of official documents (annual returns, address changes, director appointments) filed by the company."
    )
)
async def get_company_filings(
    jurisdiction_code: str,
    company_number: str,
    page: Optional[int] = None
) -> Dict[str, Any]:
    """Get company filings."""
    params = get_base_params()
    if page is not None:
        params["page"] = page
        
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(f"{BASE_URL}/companies/{jurisdiction_code}/{company_number}/filings", params=params, timeout=30.0)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            return {"error": str(e)}

@mcp.tool(
    description="A utility tool that returns a list of all country/state codes currently supported by OpenCorporates."
)
async def get_supported_jurisdictions() -> Dict[str, Any]:
    """Get supported jurisdictions."""
    params = get_base_params()
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(f"{BASE_URL}/jurisdictions", params=params, timeout=30.0)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            return {"error": str(e)}

@mcp.tool(
    description="An administrative tool that allows the agent to check the remaining daily/monthly API limits."
)
async def check_api_account_status() -> Dict[str, Any]:
    """Check API account status."""
    params = get_base_params()
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(f"{BASE_URL}/account_status", params=params, timeout=30.0)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            return {"error": str(e)}

if __name__ == "__main__":
    mcp.run(transport="stdio")
