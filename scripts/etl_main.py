#!/usr/bin/env python
# -*- coding: utf-8 -*-

import subprocess
import sys
import os
import webbrowser
import time
from threading import Thread


def verify_packages():
    """Verify required Python libraries and install missing ones."""
    packages = [
        "streamlit",
        "plotly",
        "pandas",
        "numpy",
        "pyodbc",
        "openpyxl"
    ]

    print("🔍 Verifying required libraries...\n")
    for lib in packages:
        try:
            __import__(lib)
            print(f"✔ {lib} available")
        except ImportError:
            print(f"⬇ Installing missing library: {lib}")
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", lib]
            )


def launch_browser_later(delay=3):
    """Launch web browser after a short delay."""
    time.sleep(delay)
    webbrowser.open_new("http://localhost:8501")


def start_dashboard():
    """Start the Streamlit application."""
    print("\n🚀 Launching Streamlit application")
    print("The dashboard should open automatically in your browser.")
    print("Use Ctrl + C to stop the service.")
    print("-" * 50)

    Thread(target=launch_browser_later, daemon=True).start()

    base_dir = os.path.dirname(os.path.abspath(__file__))
    app_file = os.path.join(base_dir, "dashboard.py")

    subprocess.call([
        sys.executable,
        "-m",
        "streamlit",
        "run",
        app_file
    ])


def run():
    """Application entry point."""
    print("\n" + "=" * 55)
    print("        NORTHWIND DATA VISUALIZATION")
    print("=" * 55)

    verify_packages()
    start_dashboard()


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    run()
