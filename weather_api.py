#!/usr/bin/env python3
"""
Simplified Weather API Server
============================

A minimal Flask server with only 5 essential endpoints for weather forecast processing.
Handles the complete workflow: discover → process → visualize.
"""

import os
import json
import uuid
import logging
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from flask import Flask, request, jsonify, send_file, abort
from flask_cors import CORS
from flask_compress import Compress

# Import our simplified processor
try:
    from weather_processor import (
        discover_available_runs,
        simple_process,
        parse_timestamp_flexible,
    )

    PROCESSOR_AVAILABLE = True
except ImportError:
    PROCESSOR_AVAILABLE = False

    # Define a local fallback parse function
    def parse_timestamp_flexible(time_str):
        """Parse timestamps handling both old (2025-08-31T06:05:00) and new formats"""
        if "T_" in time_str:
            # Convert 2025-08-31T_0605 to 2025-08-31T06:05:00
            parts = time_str.split("T_")
            if len(parts) == 2 and len(parts[1]) == 4:
                hour = parts[1][:2]
                minute = parts[1][2:]
                iso_str = f"{parts[0]}T{hour}:{minute}:00"
                return datetime.fromisoformat(iso_str)
        elif "_" in time_str and "T" in time_str:
            # Handle format like 2025-08-31T07_00
            if time_str.count("_") == 1 and time_str.endswith("_00"):
                base_part = time_str[:-3]  # Remove _00
                if "T" in base_part:
                    iso_str = f"{base_part}:00:00"
                    return datetime.fromisoformat(iso_str)
        return datetime.fromisoformat(time_str)


# Import weather cleanup functionality
try:
    from weather_cleanup import WeatherCleanup

    CLEANUP_AVAILABLE = True
except ImportError:
    CLEANUP_AVAILABLE = False
    logger.warning("Weather cleanup module not available")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)
app.config.update(
    SECRET_KEY=os.environ.get("FLASK_SECRET_KEY", "weather-api-key"),
    DEBUG=False,
    COMPRESS_MIMETYPES=[
        "text/html",
        "text/css",
        "text/xml",
        "application/json",
        "application/javascript",
    ],
)

# Enable CORS and compression
CORS(app, origins=["*"])
Compress(app)

# Global state for job tracking
jobs = {}
processed_data = {}

# Simple response cache for API
api_cache = {}
api_cache_ttl = {}
API_CACHE_TTL = 60  # 1 minute cache for API responses

# Cleanup manager instance
cleanup_manager = None


def get_api_cache(key: str):
    """Get cached API response if valid"""
    if key not in api_cache:
        return None

    if time.time() - api_cache_ttl.get(key, 0) > API_CACHE_TTL:
        api_cache.pop(key, None)
        api_cache_ttl.pop(key, None)
        return None

    return api_cache[key]


def set_api_cache(key: str, value):
    """Cache API response"""
    api_cache[key] = value
    api_cache_ttl[key] = time.time()


# Configuration
OUTPUT_DIR = Path("data/weather")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


@app.route("/api/runs", methods=["GET"])
def get_available_runs():
    """Get list of available forecast runs from DWD"""
    try:
        if not PROCESSOR_AVAILABLE:
            return (
                jsonify(
                    {
                        "success": False,
                        "error": "Weather processor not available",
                        "runs": [],
                    }
                ),
                500,
            )

        logger.info("🔍 Discovering available runs...")
        runs = discover_available_runs()

        # Format for frontend
        formatted_runs = []
        for run in runs:
            formatted_runs.append(
                {
                    "id": run["run_str"],
                    "display_name": run["display_name"],
                    "run_time": run["run_time"].isoformat(),
                    "age_hours": round(run["age_hours"], 1),
                    "variables": [
                        "TOT_PREC",
                        "VMAX_10M",
                    ],  # Both variables always available
                }
            )

        return jsonify(
            {"success": True, "runs": formatted_runs, "count": len(formatted_runs)}
        )

    except Exception as e:
        logger.error(f"❌ Error getting runs: {e}")
        return jsonify({"success": False, "error": str(e), "runs": []}), 500


@app.route("/api/process", methods=["POST"])
def start_processing():
    """Start processing selected forecast runs"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"success": False, "error": "No data provided"}), 400

        # Get selected runs
        selected_runs = data.get("runs", [])
        if not selected_runs:
            return jsonify({"success": False, "error": "No runs selected"}), 400

        # Create job
        job_id = str(uuid.uuid4())
        job_data = {
            "id": job_id,
            "status": "queued",
            "runs": selected_runs,
            "total_runs": len(selected_runs),
            "completed_runs": 0,
            "current_run": None,
            "progress": 0,
            "created_at": datetime.now().isoformat(),
            "started_at": None,
            "completed_at": None,
            "error": None,
            "results": [],
        }

        jobs[job_id] = job_data

        # Start processing in background
        thread = threading.Thread(target=_process_runs, args=(job_id, selected_runs))
        thread.daemon = True
        thread.start()

        logger.info(
            f"🚀 Started processing job {job_id} with {len(selected_runs)} runs"
        )

        return jsonify(
            {
                "success": True,
                "job_id": job_id,
                "message": f"Started processing {len(selected_runs)} forecast runs",
            }
        )

    except Exception as e:
        logger.error(f"❌ Error starting processing: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/status/<job_id>", methods=["GET"])
def get_processing_status(job_id):
    """Get status of processing job"""
    try:
        if job_id not in jobs:
            return jsonify({"success": False, "error": "Job not found"}), 404

        job_data = jobs[job_id]

        return jsonify({"success": True, "job": job_data})

    except Exception as e:
        logger.error(f"❌ Error getting status for {job_id}: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/data/list", methods=["GET"])
def list_available_data():
    """List all available processed forecast runs"""
    try:
        # Find all JSON forecast files (summary files)
        json_files = list(OUTPUT_DIR.glob("forecast_*.json"))

        runs = []
        for json_file in json_files:
            try:
                # Extract run ID from filename
                run_id = json_file.stem  # e.g., "forecast_2025-08-30T08%3A00"

                # Get file metadata
                file_stats = json_file.stat()

                # Load basic info from file
                with open(json_file, "r") as f:
                    data = json.load(f)

                # Extract run time and format display name
                run_str = data.get("run_time", run_id.replace("forecast_", ""))
                try:
                    # Decode URL-encoded run string for display
                    import urllib.parse

                    decoded_run = urllib.parse.unquote(run_str)

                    # Handle both old format (2025-08-31T06:00) and new format (2025-08-31T06_00)
                    if "T" in decoded_run:
                        if ":" in decoded_run:
                            # Old format with colons
                            run_dt = datetime.fromisoformat(decoded_run)
                        elif "_" in decoded_run and decoded_run.count("_") >= 1:
                            # New format with underscores - convert to standard ISO format
                            run_dt = parse_timestamp_flexible(decoded_run)
                        else:
                            raise ValueError(f"Unrecognized time format: {decoded_run}")

                        display_name = f"{run_dt.strftime('%Y-%m-%d %H:%M')} UTC"
                        age_hours = (datetime.now() - run_dt).total_seconds() / 3600
                    else:
                        display_name = decoded_run
                        age_hours = 0
                except Exception as e:
                    logger.warning(f"Error parsing run time {run_str}: {e}")
                    display_name = run_str
                    age_hours = 0

                runs.append(
                    {
                        "id": run_id,
                        "run_time": run_str,
                        "display_name": display_name,
                        "age_hours": round(age_hours, 1),
                        "file_size": file_stats.st_size,
                        "modified_at": datetime.fromtimestamp(
                            file_stats.st_mtime
                        ).isoformat(),
                        "variables": list(data.get("variables", {}).keys()),
                    }
                )

            except Exception as e:
                logger.warning(f"⚠️ Error processing {json_file}: {e}")
                continue

        # Sort by modification time, newest first
        runs.sort(key=lambda x: x["modified_at"], reverse=True)

        return jsonify({"success": True, "runs": runs, "count": len(runs)})

    except Exception as e:
        logger.error(f"❌ Error listing available data: {e}")
        return jsonify({"success": False, "error": str(e), "runs": []}), 500


@app.route("/api/data/<path:run_id>", methods=["GET"])
def get_specific_run_data(run_id):
    """Get data for a specific forecast run with caching"""
    cache_key = f"run_data_{run_id}"

    # Try cache first
    cached_response = get_api_cache(cache_key)
    if cached_response:
        logger.info(f"🚀 Serving cached data for run {run_id}")
        return cached_response

    try:
        # Handle URL encoding - try both encoded and decoded versions
        import urllib.parse

        # First try the run_id as provided
        json_file = OUTPUT_DIR / f"{run_id}.json"

        # If not found, try URL-encoded version
        if not json_file.exists():
            encoded_run_id = urllib.parse.quote(run_id, safe="")
            json_file = OUTPUT_DIR / f"{encoded_run_id}.json"

        # If still not found, try replacing colons with %3A
        if not json_file.exists():
            url_encoded_run_id = run_id.replace(":", "%3A")
            json_file = OUTPUT_DIR / f"{url_encoded_run_id}.json"

        if not json_file.exists():
            return (
                jsonify(
                    {"success": False, "error": f"Run {run_id} not found", "data": None}
                ),
                404,
            )

        # Load summary data
        with open(json_file, "r") as f:
            data = json.load(f)

        # Process variables (no timestamp stripping - keeping all forecast data)
        if "variables" in data:
            data["variables"] = strip_first_timestamp(data["variables"])

        # Add derived variables from TOT_PREC if it exists
        if "TOT_PREC" in data.get("variables", {}):
            # Pass ensemble data if available for more accurate accumulated precipitation
            ensemble_data = data.get("ensembles", None)
            data["variables"] = add_derived_precipitation_variables(
                data["variables"], ensemble_data
            )

        # Load individual ensemble files for more detailed data
        # Use the same filename logic for the ensemble directory
        run_dir_name = json_file.stem  # Get the actual filename without extension
        ensemble_dir = OUTPUT_DIR / run_dir_name

        if ensemble_dir.exists():
            # Load ensemble data
            ensembles = {}

            # Load precipitation ensembles
            prec_files = list(ensemble_dir.glob("TOT_PREC_ensemble_*.json"))
            if prec_files:
                ensembles["TOT_PREC"] = []
                for prec_file in sorted(prec_files):
                    with open(prec_file, "r") as f:
                        ensemble_data = json.load(f)
                        ensembles["TOT_PREC"].append(ensemble_data)

            # Load wind speed ensembles
            wind_files = list(ensemble_dir.glob("VMAX_10M_ensemble_*.json"))
            if wind_files:
                ensembles["VMAX_10M"] = []
                for wind_file in sorted(wind_files):
                    with open(wind_file, "r") as f:
                        ensemble_data = json.load(f)
                        ensembles["VMAX_10M"].append(ensemble_data)

            # Add ensemble data to response
            data["ensembles"] = ensembles

        # Add file metadata
        file_stats = json_file.stat()
        data["file_info"] = {
            "filename": json_file.name,
            "size_bytes": file_stats.st_size,
            "modified_at": datetime.fromtimestamp(file_stats.st_mtime).isoformat(),
            "has_ensemble_data": "ensembles" in data,
        }

        response_data = {
            "success": True,
            "data": data,
            "message": f"Loaded data from {json_file.name}",
        }

        # Cache the successful response
        set_api_cache(cache_key, jsonify(response_data))

        return jsonify(response_data)

    except Exception as e:
        logger.error(f"❌ Error loading run {run_id}: {e}")
        return jsonify({"success": False, "error": str(e), "data": None}), 500


@app.route("/api/data", methods=["GET"])
def get_processed_data():
    """Get latest processed forecast data for visualization (backward compatibility)"""
    try:
        # Find the most recent JSON file (summary files)
        json_files = list(OUTPUT_DIR.glob("forecast_*.json"))
        if not json_files:
            return (
                jsonify(
                    {
                        "success": False,
                        "error": "No processed data found. Process some forecast runs first.",
                        "data": None,
                    }
                ),
                404,
            )

        # Get the most recent file
        latest_file = max(json_files, key=lambda x: x.stat().st_mtime)

        # Use the specific run endpoint for consistency
        run_id = latest_file.stem
        return get_specific_run_data(run_id)

    except Exception as e:
        logger.error(f"❌ Error loading processed data: {e}")
        return jsonify({"success": False, "error": str(e), "data": None}), 500


@app.route("/", methods=["GET"])
def serve_dashboard():
    """Serve the main dashboard HTML"""
    dashboard_file = Path(__file__).parent / "weather_dashboard.html"

    if dashboard_file.exists():
        return send_file(dashboard_file)
    else:
        return """
        <html>
        <head><title>Weather Dashboard</title></head>
        <body>
            <h1>Weather Dashboard</h1>
            <p>Dashboard file not found. Please create weather_dashboard.html</p>
        </body>
        </html>
        """


def add_derived_precipitation_variables(
    variables: Dict, ensembles: Dict = None
) -> Dict:
    """Add derived precipitation variables from existing TOT_PREC data using vectorized operations

    Args:
        variables: The main variables dict containing ensemble statistics
        ensembles: Optional dict containing individual ensemble data with accumulated_values
    """
    if "TOT_PREC" not in variables:
        return variables

    tot_prec = variables["TOT_PREC"]
    if "ensemble_statistics" not in tot_prec:
        return variables

    import numpy as np
    from datetime import datetime, timedelta

    # Parse times (first value already stripped in caller)
    times = [parse_timestamp_flexible(t) for t in tot_prec["times"]]

    # Calculate time step in minutes
    if len(times) > 1:
        time_step = (times[1] - times[0]).total_seconds() / 60  # minutes
        steps_per_hour = int(60 / time_step) if time_step > 0 else 12
    else:
        steps_per_hour = 12  # default

    # CORRECTED: Compute accumulated precipitation from ORIGINAL accumulated values
    accum_stats = {}

    # If we have access to individual ensembles with original accumulated_values, use them
    if ensembles and "TOT_PREC" in ensembles:
        logger.info("🔧 Computing accumulated stats from original accumulated values")

        # Collect original accumulated values from all ensembles
        all_accumulated = []
        for ensemble in ensembles["TOT_PREC"]:
            if (
                "accumulated_values" in ensemble
                and len(ensemble["accumulated_values"]) > 0
            ):
                # Keep all accumulated values - no cropping needed
                accum_vals = ensemble["accumulated_values"]
                all_accumulated.append(accum_vals)

        if all_accumulated:
            # Compute statistics from original accumulated values
            accumulated_array = np.array(all_accumulated, dtype=np.float32)
            percentiles = [5, 10, 25, 50, 75, 90, 95]
            percentile_values = np.percentile(accumulated_array, percentiles, axis=0)

            accum_stats = {
                "mean": np.mean(accumulated_array, axis=0).tolist(),
                "median": percentile_values[3].tolist(),
                "p05": percentile_values[0].tolist(),
                "p10": percentile_values[1].tolist(),
                "p25": percentile_values[2].tolist(),
                "p50": percentile_values[3].tolist(),
                "p75": percentile_values[4].tolist(),
                "p90": percentile_values[5].tolist(),
                "p95": percentile_values[6].tolist(),
                "min": np.min(accumulated_array, axis=0).tolist(),
                "max": np.max(accumulated_array, axis=0).tolist(),
                "std": np.std(accumulated_array, axis=0, dtype=np.float32).tolist(),
            }
    else:
        # Fallback: compute from rates (less accurate but better than nothing)
        logger.warning("⚠️ Using fallback: computing accumulated from rates")
        stats = tot_prec["ensemble_statistics"]  # These are rates
        time_step_hours = time_step / 60.0

        for key, rate_values in stats.items():
            if isinstance(rate_values, list) and len(rate_values) > 0:
                rate_array = np.array(rate_values, dtype=np.float32)
                accum_values = np.cumsum(rate_array) * time_step_hours
                accum_stats[key] = accum_values.tolist()

    # Vectorized computation for hourly precipitation bins
    hourly_stats = {}

    for key, values in stats.items():
        if isinstance(values, list) and len(values) > 0:
            values_array = np.array(values, dtype=np.float32)

            # Pad array to ensure it's divisible by steps_per_hour
            remainder = len(values_array) % steps_per_hour
            if remainder != 0:
                padding_size = steps_per_hour - remainder
                values_array = np.pad(
                    values_array, (0, padding_size), mode="constant", constant_values=0
                )

            # Reshape and sum across hourly bins (vectorized)
            reshaped = values_array.reshape(-1, steps_per_hour)
            hourly_sums = np.sum(reshaped, axis=1)
            hourly_stats[key] = hourly_sums[
                : len(times) // steps_per_hour
                + (1 if len(times) % steps_per_hour else 0)
            ].tolist()

    # Create hourly times (vectorized bin end time calculation)
    hourly_times = []
    if len(times) > 0:
        # Calculate bin end indices efficiently
        bin_end_indices = np.arange(steps_per_hour - 1, len(times), steps_per_hour)
        # Handle the last incomplete bin
        if len(times) % steps_per_hour != 0:
            bin_end_indices = np.append(bin_end_indices, len(times) - 1)

        # Use vectorized indexing to get times
        hourly_times = [times[i].isoformat() for i in bin_end_indices if i < len(times)]

    # Add derived variables (first timestamp already excluded)
    variables["TOT_PREC_ACCUM"] = {
        "name": "Accumulated Precipitation",
        "unit": "mm",
        "num_ensembles": tot_prec["num_ensembles"],
        "times": tot_prec["times"],
        "ensemble_statistics": accum_stats,
    }

    variables["TOT_PREC_1H"] = {
        "name": "1-Hour Precipitation",
        "unit": "mm",
        "num_ensembles": tot_prec["num_ensembles"],
        "times": hourly_times,
        "ensemble_statistics": hourly_stats,
    }

    return variables


def strip_first_timestamp(variables: Dict) -> Dict:
    """
    No longer strips timestamps - returns variables unchanged.

    Previously stripped first timestamp for (T0, T1] interval notation, but this was causing
    unnecessary data loss, especially for VMAX_10M (wind speed) which is instantaneous.
    TOT_PREC deaccumulation already handles the first timestamp correctly.
    """
    # Return variables unchanged - no cropping needed
    return variables


def _process_runs(job_id: str, selected_runs: List[str]):
    """Background function to process forecast runs"""
    try:
        job_data = jobs[job_id]

        # Mark as started
        job_data.update(
            {"status": "processing", "started_at": datetime.now().isoformat()}
        )

        logger.info(f"📊 Processing {len(selected_runs)} runs for job {job_id}")

        for i, run_str in enumerate(selected_runs):
            try:
                # Update current status
                job_data.update(
                    {
                        "current_run": run_str,
                        "progress": int((i / len(selected_runs)) * 100),
                    }
                )

                logger.info(f"📈 Processing run {i+1}/{len(selected_runs)}: {run_str}")

                # Process the run
                result_file = simple_process(run_str, str(OUTPUT_DIR))

                if result_file:
                    job_data["results"].append(
                        {"run": run_str, "file": result_file, "status": "success"}
                    )
                    job_data["completed_runs"] += 1
                    logger.info(f"✅ Successfully processed {run_str}")
                else:
                    job_data["results"].append(
                        {
                            "run": run_str,
                            "file": None,
                            "status": "failed",
                            "error": "Processing failed",
                        }
                    )
                    logger.warning(f"⚠️ Failed to process {run_str}")

                # Small delay between runs
                time.sleep(1)

            except Exception as e:
                logger.error(f"❌ Error processing run {run_str}: {e}")
                job_data["results"].append(
                    {"run": run_str, "file": None, "status": "failed", "error": str(e)}
                )

        # Mark as completed
        job_data.update(
            {
                "status": "completed",
                "progress": 100,
                "current_run": None,
                "completed_at": datetime.now().isoformat(),
            }
        )

        success_count = sum(1 for r in job_data["results"] if r["status"] == "success")
        logger.info(
            f"🎉 Job {job_id} completed: {success_count}/{len(selected_runs)} runs successful"
        )

    except Exception as e:
        logger.error(f"❌ Error in background processing for job {job_id}: {e}")
        jobs[job_id].update(
            {
                "status": "failed",
                "error": str(e),
                "completed_at": datetime.now().isoformat(),
            }
        )


# Proxy endpoint for measured precipitation data
@app.route("/api/stations/<int:station_id>/timeseries", methods=["GET"])
def get_station_timeseries(station_id):
    """Proxy measured precipitation timeseries for station to bypass CORS"""
    try:
        variable = request.args.get("variable", "precipitation")
        period = request.args.get("period", "6h")

        if variable != "precipitation":
            return (
                jsonify(
                    {"success": False, "error": f"Variable {variable} not supported"}
                ),
                400,
            )

        if station_id != 11816:
            return (
                jsonify({"success": False, "error": f"Station {station_id} not found"}),
                404,
            )

        # Proxy the request to the actual API server
        import requests

        try:
            # Try to fetch from the actual API
            api_url = f"http://localhost:8080/api/stations/{station_id}/timeseries"
            params = {"variable": variable, "period": period}

            logger.info(f"Proxying request to: {api_url} with params: {params}")

            response = requests.get(api_url, params=params, timeout=10)

            if response.status_code == 200:
                # Forward the successful response
                api_data = response.json()
                logger.info(
                    f"Successfully proxied precipitation data: {len(api_data.get('data_points', []))} points"
                )
                return jsonify(api_data)
            else:
                logger.warning(
                    f"API returned status {response.status_code}, falling back to mock data"
                )
                raise Exception(f"API returned {response.status_code}")

        except Exception as proxy_error:
            logger.info(f"Real API not available ({proxy_error}), returning empty data")

            # Return empty data if real API is not available
            return jsonify(
                {
                    "success": True,
                    "station_id": station_id,
                    "variable": variable,
                    "period": period,
                    "unit": "mm/h",
                    "data_points": [],
                    "count": 0,
                    "note": "Real API not available - no data to display",
                }
            )

    except Exception as e:
        logger.error(f"❌ Error getting station data: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


# Proxy endpoint for wind data (similar to precipitation)
@app.route("/api/stations/<int:station_id>/wind", methods=["GET"])
def get_station_wind(station_id):
    """Proxy measured wind data for station to bypass CORS"""
    try:
        period = request.args.get("period", "6h")

        if station_id != 11816:
            return (
                jsonify({"success": False, "error": f"Station {station_id} not found"}),
                404,
            )

        # Proxy the request to the actual API server
        import requests

        try:
            # Try to fetch from the actual API
            api_url = f"http://localhost:8080/api/stations/{station_id}/wind"
            params = {"period": period}

            logger.info(f"Proxying wind request to: {api_url} with params: {params}")

            response = requests.get(api_url, params=params, timeout=10)

            if response.status_code == 200:
                # Forward the successful response
                api_data = response.json()
                logger.info(
                    f"Successfully proxied wind data: {len(api_data.get('data_points', []))} points"
                )
                return jsonify(api_data)
            else:
                logger.warning(
                    f"Wind API returned status {response.status_code}, returning empty data"
                )
                raise Exception(f"API returned {response.status_code}")

        except Exception as proxy_error:
            logger.info(
                f"Real wind API not available ({proxy_error}), returning empty data"
            )

            # Return empty data if real API is not available
            return jsonify(
                {
                    "success": True,
                    "station_id": station_id,
                    "period": period,
                    "data_points": [],  # Empty data
                    "count": 0,
                    "note": "Real wind API not available - no data to display",
                }
            )

    except Exception as e:
        logger.error(f"❌ Error getting wind data: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


# Health check endpoint
@app.route("/api/health", methods=["GET"])
def health_check():
    """Health check endpoint"""
    return jsonify(
        {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "processor_available": PROCESSOR_AVAILABLE,
            "cleanup_available": CLEANUP_AVAILABLE,
            "active_jobs": len(
                [j for j in jobs.values() if j["status"] in ["queued", "processing"]]
            ),
        }
    )


# Cleanup endpoint
@app.route("/api/cleanup", methods=["POST"])
def trigger_cleanup():
    """Manually trigger weather data cleanup"""
    try:
        if not CLEANUP_AVAILABLE:
            return (
                jsonify({"success": False, "error": "Cleanup module not available"}),
                500,
            )

        # Get retention hours from request or use default
        data = request.get_json() or {}
        retention_hours = data.get("retention_hours", 6)

        if retention_hours < 1 or retention_hours > 168:  # Max 1 week
            return (
                jsonify(
                    {
                        "success": False,
                        "error": "retention_hours must be between 1 and 168 (1 week)",
                    }
                ),
                400,
            )

        logger.info(f"🧹 Manual cleanup triggered with {retention_hours}h retention")

        # Run cleanup
        cleanup = WeatherCleanup(retention_hours=retention_hours)
        result = cleanup.cleanup_old_data()

        return jsonify(
            {
                "success": True,
                "message": f"Cleanup completed with {retention_hours}h retention",
                "stats": result,
            }
        )

    except Exception as e:
        logger.error(f"❌ Cleanup failed: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/cleanup/status", methods=["GET"])
def get_cleanup_status():
    """Get current data status without performing cleanup"""
    try:
        if not CLEANUP_AVAILABLE:
            return (
                jsonify({"success": False, "error": "Cleanup module not available"}),
                500,
            )

        cleanup = WeatherCleanup()
        status = cleanup.get_data_status()

        return jsonify({"success": True, "data_status": status})

    except Exception as e:
        logger.error(f"❌ Error getting cleanup status: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


# Optional: Clean up old jobs (runs every hour)
def cleanup_old_jobs():
    """Clean up completed jobs older than 1 hour"""
    cutoff_time = datetime.now().timestamp() - 3600  # 1 hour ago

    jobs_to_remove = []
    for job_id, job_data in jobs.items():
        if job_data["status"] in ["completed", "failed"]:
            created_at = datetime.fromisoformat(job_data["created_at"]).timestamp()
            if created_at < cutoff_time:
                jobs_to_remove.append(job_id)

    for job_id in jobs_to_remove:
        del jobs[job_id]
        logger.info(f"🧹 Cleaned up old job: {job_id}")


def startup_cleanup():
    """Perform automatic cleanup on server startup"""
    if not CLEANUP_AVAILABLE:
        logger.info("🚫 Weather cleanup not available, skipping startup cleanup")
        return

    try:
        logger.info("🧹 Running startup cleanup (12-hour retention)...")
        cleanup = WeatherCleanup(retention_hours=6)
        result = cleanup.cleanup_old_data()

        if (
            result.get("total_files_deleted", 0) > 0
            or result.get("total_directories_deleted", 0) > 0
        ):
            space_mb = result.get("total_space_freed_bytes", 0) / (1024**2)
            logger.info(
                f"✅ Startup cleanup completed: "
                f"{result.get('total_files_deleted', 0)} files and "
                f"{result.get('total_directories_deleted', 0)} directories deleted, "
                f"{space_mb:.1f} MB freed"
            )
        else:
            logger.info("✅ Startup cleanup completed: No old files found to delete")

    except Exception as e:
        logger.error(f"❌ Startup cleanup failed: {e}")


def background_cleanup_worker():
    """Background worker that runs periodic cleanup every 6 hours"""
    if not CLEANUP_AVAILABLE:
        return

    while True:
        try:
            # Wait 6 hours (21600 seconds)
            time.sleep(21600)

            logger.info("🔄 Running scheduled background cleanup...")
            cleanup = WeatherCleanup(retention_hours=12)
            result = cleanup.cleanup_old_data()

            if result.get("total_files_deleted", 0) > 0:
                space_mb = result.get("total_space_freed_bytes", 0) / (1024**2)
                logger.info(
                    f"✅ Background cleanup completed: "
                    f"{result.get('total_files_deleted', 0)} files deleted, "
                    f"{space_mb:.1f} MB freed"
                )

        except Exception as e:
            logger.error(f"❌ Background cleanup failed: {e}")
            # Sleep for 1 hour before retrying
            time.sleep(3600)


if __name__ == "__main__":
    import sys

    # Check if processor is available
    if not PROCESSOR_AVAILABLE:
        logger.error(
            "❌ Weather processor not available. Please ensure weather_processor.py is working."
        )
        sys.exit(1)

    # Start cleanup timer
    import atexit
    import signal

    def cleanup_handler():
        logger.info("🛑 Shutting down weather API server")

    atexit.register(cleanup_handler)
    signal.signal(signal.SIGTERM, lambda s, f: cleanup_handler())

    # Start server
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", 8080))

    logger.info(f"🌦️  Starting Weather API Server on {host}:{port}")
    logger.info(f"📊 Data directory: {OUTPUT_DIR.absolute()}")
    logger.info(f"🔧 Processor available: {PROCESSOR_AVAILABLE}")
    logger.info(f"🧹 Cleanup available: {CLEANUP_AVAILABLE}")

    # Run startup cleanup
    startup_cleanup()

    # Start background cleanup worker
    if CLEANUP_AVAILABLE:
        cleanup_thread = threading.Thread(target=background_cleanup_worker, daemon=True)
        cleanup_thread.start()
        logger.info("🔄 Background cleanup worker started (runs every 6 hours)")

    app.run(host=host, port=port, debug=False, threaded=True)
