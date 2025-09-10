"""
Data discovery utilities for ICON-D2-RUC-EPS
"""
import requests
import re
import json
from datetime import datetime
from bs4 import BeautifulSoup
from pathlib import Path
import sys
sys.path.append('..')
from config import *

def get_available_run_times(limit=None):
    """
    Discover available forecast run times by listing the remote directory.
    
    Args:
        limit (int): Maximum number of runs to return (most recent first)
    
    Returns:
        list: List of available run time strings in URL format
    """
    if limit is None:
        limit = DEFAULT_RUNS_LIMIT
        
    print(f"Fetching available runs from: {BASE_DATA_URL}")
    
    try:
        response = requests.get(BASE_DATA_URL)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        run_time_pattern = r'(\d{4}-\d{2}-\d{2}T\d{2}%3A00)/'
        run_times = []
        
        for link in soup.find_all('a'):
            href = link.get('href', '')
            match = re.search(run_time_pattern, href)
            if match:
                run_time_str = match.group(1)
                run_times.append(run_time_str)
        
        run_times.sort(reverse=True)
        available_runs = run_times[:limit]
        
        print(f"Found {len(run_times)} total runs, returning {len(available_runs)} most recent:")
        for run_time in available_runs:
            readable_time = run_time.replace('%3A', ':')
            print(f"  {readable_time}")
        
        return available_runs
        
    except Exception as e:
        print(f"Error fetching available runs: {e}")
        return []

def get_available_ensembles(run_time_str):
    """
    Get all available ensemble members for a specific run time.
    
    Args:
        run_time_str (str): Run time in URL format
        
    Returns:
        list: List of ensemble member strings
    """
    ensemble_url = BASE_ENSEMBLE_URL.format(run_time=run_time_str)
    
    try:
        response = requests.get(ensemble_url)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        ensembles = []
        for link in soup.find_all('a'):
            href = link.get('href', '')
            if href.endswith('/') and href[:-1].isdigit():
                ensemble_num = href[:-1]
                ensembles.append(ensemble_num)
        
        ensembles.sort(key=int)
        return ensembles
        
    except Exception as e:
        print(f"Error fetching ensembles for {run_time_str}: {e}")
        return []

def get_available_steps(run_time_str, ensemble):
    """
    Get all available forecast steps for a specific run time and ensemble member.
    
    Args:
        run_time_str (str): Run time in URL format
        ensemble (str): Ensemble member number
        
    Returns:
        list: List of available step filenames
    """
    step_url = BASE_STEP_URL.format(run_time=run_time_str, ensemble=ensemble)
    
    try:
        response = requests.get(step_url)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        steps = []
        for link in soup.find_all('a'):
            href = link.get('href', '')
            if href.endswith('.grib2') and 'PT' in href:
                steps.append(href)
        
        steps.sort()
        return steps
        
    except Exception as e:
        print(f"Error fetching steps for {run_time_str}, ensemble {ensemble}: {e}")
        return []

def parse_step_time(step_str):
    """
    Parse PTXXXHXXM.grib2 format to hours and minutes.
    
    Args:
        step_str (str): Step string like 'PT000H00M.grib2'
        
    Returns:
        tuple: (hours, minutes) as integers
    """
    import re
    match = re.match(r'PT(\d{3})H(\d{2})M\.grib2', step_str)
    if match:
        return int(match.group(1)), int(match.group(2))
    return 0, 0

def filter_steps_by_interval(steps, interval_minutes=15):
    """
    Filter forecast steps to specific minute intervals.
    
    Args:
        steps (list): List of step strings (e.g., ['PT000H00M.grib2', 'PT000H05M.grib2', ...])
        interval_minutes (int): Keep only steps at these minute intervals (15 = 00, 15, 30, 45)
    
    Returns:
        list: Filtered steps at specified intervals
    """
    filtered = []
    
    for step in steps:
        hours, minutes = parse_step_time(step)
        
        # Check if minutes are at the desired interval
        if minutes % interval_minutes == 0:
            filtered.append(step)
    
    return filtered

def filter_steps_by_time_window_and_interval(steps, 
                                            forecast_hours=None, 
                                            interval_minutes=15):
    """
    Filter steps by both time window and minute interval.
    
    Args:
        steps (list): List of step strings
        forecast_hours (float): Maximum forecast hours (None = all)
        interval_minutes (int): Minute interval to keep (15 = 00, 15, 30, 45)
    
    Returns:
        list: Filtered steps
    """
    filtered = []
    max_minutes = forecast_hours * 60 if forecast_hours else float('inf')
    
    for step in steps:
        hours, minutes = parse_step_time(step)
        total_minutes = hours * 60 + minutes
        
        # Check both conditions: within time window AND at correct interval
        if total_minutes <= max_minutes and minutes % interval_minutes == 0:
            filtered.append(step)
    
    return filtered

def describe_step_filtering(steps, filtered_steps):
    """
    Provide summary of filtering results.
    
    Args:
        steps (list): Original list of steps
        filtered_steps (list): Filtered list of steps
        
    Returns:
        dict: Summary statistics
    """
    reduction = (1 - len(filtered_steps) / len(steps)) * 100 if steps else 0
    return {
        'original_count': len(steps),
        'filtered_count': len(filtered_steps),
        'reduction_percent': reduction,
        'time_range': get_time_range(filtered_steps) if filtered_steps else None
    }

def get_time_range(steps):
    """
    Get time range covered by steps.
    
    Args:
        steps (list): List of step strings
        
    Returns:
        dict: Time range information
    """
    if not steps:
        return None
    
    times = []
    for step in steps:
        hours, minutes = parse_step_time(step)
        times.append(hours * 60 + minutes)
    
    return {
        'start_hours': min(times) / 60,
        'end_hours': max(times) / 60,
        'total_hours': (max(times) - min(times)) / 60,
        'count': len(steps)
    }

def discover_all_data(num_runs=None):
    """
    Discover all available data (runs, ensembles, steps) and return structured results.
    
    Args:
        num_runs (int): Number of recent runs to discover
        
    Returns:
        dict: Nested dictionary with run_time -> ensemble -> steps structure
    """
    if num_runs is None:
        num_runs = DEFAULT_RUNS_LIMIT
    
    print("Starting comprehensive data discovery...")
    
    # Get available runs
    available_runs = get_available_run_times(limit=num_runs)
    
    if not available_runs:
        print("No runs found!")
        return {}
    
    # Discover ensembles and steps for each run
    run_ensemble_steps = {}
    
    for run_time_str in available_runs:
        readable_time = run_time_str.replace('%3A', ':')
        print(f"\nDiscovering data for {readable_time}...")
        
        ensembles = get_available_ensembles(run_time_str)
        if not ensembles:
            print(f"  No ensembles found for {readable_time}")
            continue
            
        print(f"  Found {len(ensembles)} ensemble members: {ensembles}")
        
        run_ensemble_steps[run_time_str] = {}
        
        # Get steps for first few ensembles (assuming all have same steps)
        sample_ensembles = ensembles[:3]  # Check first 3 ensembles
        reference_steps = None
        
        for i, ensemble in enumerate(sample_ensembles):
            steps = get_available_steps(run_time_str, ensemble)
            run_ensemble_steps[run_time_str][ensemble] = steps
            
            if i == 0:
                reference_steps = steps
                print(f"  Found {len(steps)} forecast steps: {steps[:3]}{'...' if len(steps) > 3 else ''}")
        
        # Copy reference steps to all other ensembles (performance optimization)
        if reference_steps:
            for ensemble in ensembles[3:]:
                run_ensemble_steps[run_time_str][ensemble] = reference_steps
    
    print(f"\nDiscovery completed for {len(run_ensemble_steps)} runs")
    return run_ensemble_steps

def save_discovery_results(results, cache_file=None):
    """
    Save discovery results to JSON cache file.
    
    Args:
        results (dict): Discovery results from discover_all_data()
        cache_file (Path): Path to cache file (optional)
    """
    if cache_file is None:
        cache_file = DISCOVERY_CACHE_FILE
    
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Add timestamp to results
    cache_data = {
        'timestamp': datetime.now().isoformat(),
        'results': results
    }
    
    try:
        with open(cache_file, 'w') as f:
            json.dump(cache_data, f, indent=2)
        print(f"Discovery results saved to: {cache_file}")
        
    except Exception as e:
        print(f"Error saving discovery results: {e}")

def load_discovery_results(cache_file=None, max_age_hours=1):
    """
    Load discovery results from JSON cache file if recent enough.
    
    Args:
        cache_file (Path): Path to cache file (optional)
        max_age_hours (int): Maximum age in hours before cache is invalid
        
    Returns:
        dict or None: Discovery results if cache is valid, None otherwise
    """
    if cache_file is None:
        cache_file = DISCOVERY_CACHE_FILE
    
    if not cache_file.exists():
        return None
    
    try:
        with open(cache_file, 'r') as f:
            cache_data = json.load(f)
        
        # Check cache age
        timestamp = datetime.fromisoformat(cache_data['timestamp'])
        age_hours = (datetime.now() - timestamp).total_seconds() / 3600
        
        if age_hours > max_age_hours:
            print(f"Discovery cache is {age_hours:.1f} hours old, too old to use")
            return None
        
        print(f"Using cached discovery results (age: {age_hours:.1f} hours)")
        return cache_data['results']
        
    except Exception as e:
        print(f"Error loading discovery cache: {e}")
        return None

def print_discovery_summary(results):
    """
    Print a summary of discovery results.
    
    Args:
        results (dict): Discovery results from discover_all_data()
    """
    if not results:
        print("No discovery results to summarize")
        return
    
    print("\n" + "="*70)
    print("DISCOVERY SUMMARY")
    print("="*70)
    
    total_files = 0
    
    for i, (run_time_str, ensembles_data) in enumerate(results.items(), 1):
        readable_time = run_time_str.replace('%3A', ':')
        ensembles = list(ensembles_data.keys())
        
        if not ensembles:
            continue
        
        # Get steps from first ensemble
        first_ensemble = ensembles[0]
        steps = ensembles_data[first_ensemble]
        
        run_files = len(ensembles) * len(steps)
        total_files += run_files
        
        print(f"\n{i}. Run: {readable_time}")
        print(f"   Ensembles: {len(ensembles)} members ({min(ensembles)}-{max(ensembles)})")
        print(f"   Steps: {len(steps)} forecast hours")
        print(f"   Range: {steps[0] if steps else 'N/A'} to {steps[-1] if steps else 'N/A'}")
        print(f"   Total files: {run_files}")
    
    print(f"\nTOTAL FILES AVAILABLE: {total_files}")
    
    if results:
        first_run = list(results.keys())[0]
        first_ensembles = list(results[first_run].keys())
        first_steps = results[first_run][first_ensembles[0]] if first_ensembles else []
        
        print(f"\nDataset dimensions:")
        print(f"  - Runs: {len(results)}")
        print(f"  - Ensembles: {len(first_ensembles)}")  
        print(f"  - Steps: {len(first_steps)}")
    
    print("="*70)