#!/usr/bin/env python3
"""Wrapper to load .env and run export script"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Load .env file
env_path = Path(__file__).parent / '.env'
load_dotenv(env_path)

# Now run the export script
import export_ld_to_excel_simple
export_ld_to_excel_simple.main()
