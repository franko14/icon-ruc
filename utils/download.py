"""
Download utilities for ICON-D2-RUC-EPS GRIB2 files
"""
import requests
from pathlib import Path
from datetime import datetime
import time
import sys
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from typing import List, Dict, Optional, Tuple, Union
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from functools import partial
import os

# Try to import tqdm for progress bars, fallback to basic progress if not available
try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    print("Warning: tqdm not available. Install with 'pip install tqdm' for enhanced progress bars.")

sys.path.append('..')
from config import *

# Thread-local storage for session reuse
_thread_local = threading.local()

def get_session_with_retries():
    """Get a requests session with retry strategy for the current thread."""
    if not hasattr(_thread_local, 'session'):
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=20)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        _thread_local.session = session
    
    return _thread_local.session

def download_grib_file(run_time_str, ensemble, step, download_dir=None):
    """
    Download GRIB2 file for a specific run time, ensemble member, and step from DWD server.
    
    Args:
        run_time_str (str): Run time in URL format (e.g., "2025-08-29T09%3A00")
        ensemble (str): Ensemble member number (e.g., "01", "02", ..., "20")
        step (str): Forecast step (e.g., "PT014H00M.grib2")
        download_dir (Path): Directory to save the downloaded file
    
    Returns:
        str: Path to the downloaded file, or None if download failed
    """
    if download_dir is None:
        download_dir = RAW_DATA_DIR
    
    # Create download directory if it doesn't exist
    Path(download_dir).mkdir(parents=True, exist_ok=True)
    
    # Construct the full URL
    url = BASE_DOWNLOAD_URL.format(run_time=run_time_str, ensemble=ensemble, step=step)
    
    # Create filename from run time, ensemble, and step
    readable_time = run_time_str.replace('%3A', ':')
    run_time_dt = datetime.strptime(readable_time, '%Y-%m-%dT%H:%M')
    filename = GRIB_FILENAME_PATTERN.format(
        run_date=run_time_dt.strftime('%Y%m%d'),
        run_hour=run_time_dt.strftime('%H'),
        ensemble=ensemble,
        step=step
    )
    filepath = Path(download_dir) / filename
    
    # Skip if file already exists
    if filepath.exists():
        return str(filepath)
    
    try:
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        
        # Get file size for progress reporting
        total_size = int(response.headers.get('content-length', 0))
        
        with open(filepath, 'wb') as f:
            downloaded = 0
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                downloaded += len(chunk)
        
        return str(filepath)
        
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")
        return None

def parallel_download_grib_file(args: Tuple[str, str, str, Optional[Path]]) -> Optional[str]:
    """
    Thread-safe wrapper for downloading a single GRIB file.
    
    Args:
        args: Tuple of (run_time_str, ensemble, step, download_dir)
    
    Returns:
        str: Path to the downloaded file, or None if download failed
    """
    run_time_str, ensemble, step, download_dir = args
    
    if download_dir is None:
        download_dir = RAW_DATA_DIR
    
    # Create download directory if it doesn't exist
    Path(download_dir).mkdir(parents=True, exist_ok=True)
    
    # Construct the full URL
    url = BASE_DOWNLOAD_URL.format(run_time=run_time_str, ensemble=ensemble, step=step)
    
    # Create filename from run time, ensemble, and step
    readable_time = run_time_str.replace('%3A', ':')
    run_time_dt = datetime.strptime(readable_time, '%Y-%m-%dT%H:%M')
    filename = GRIB_FILENAME_PATTERN.format(
        run_date=run_time_dt.strftime('%Y%m%d'),
        run_hour=run_time_dt.strftime('%H'),
        ensemble=ensemble,
        step=step
    )
    filepath = Path(download_dir) / filename
    
    # Skip if file already exists
    if filepath.exists():
        return str(filepath)
    
    try:
        session = get_session_with_retries()
        response = session.get(url, stream=True, timeout=60)
        response.raise_for_status()
        
        # Get file size for progress reporting
        total_size = int(response.headers.get('content-length', 0))
        
        with open(filepath, 'wb') as f:
            downloaded = 0
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)
                    downloaded += len(chunk)
        
        return str(filepath)
        
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error downloading {url}: {e}")
        return None

def batch_download(run_ensemble_steps, max_files=None, progress_interval=10):
    """
    Download GRIB2 files for multiple runs, ensemble members, and steps.
    
    Args:
        run_ensemble_steps: Either a dict (from discovery) or list of tuples [(run_time, ensemble, step), ...]
        max_files (int): Maximum number of files to download (for testing)
        progress_interval (int): Show progress every N files
    
    Returns:
        list: List of successfully downloaded file paths
    """
    if not run_ensemble_steps:
        print("No runs found for download!")
        return []
    
    downloaded_files = []
    downloaded_count = 0
    start_time = time.time()
    
    # Handle both formats: dict (old format) or list of tuples (new format)
    if isinstance(run_ensemble_steps, list):
        # New format: list of (run_time, ensemble, step) tuples
        download_list = run_ensemble_steps
        
        if max_files:
            download_list = download_list[:max_files]
            print(f"Limiting download to first {max_files} files")
        
        total_downloads = len(download_list)
        print(f"Downloading {total_downloads} GRIB2 files...")
        
        for i, (run_time_str, ensemble, step) in enumerate(download_list):
            try:
                filepath = download_grib_file(run_time_str, ensemble, step)
                if filepath and Path(filepath).exists():
                    downloaded_files.append(filepath)
                    downloaded_count += 1
                    
                    # Progress updates
                    if downloaded_count % progress_interval == 0:
                        elapsed_time = time.time() - start_time
                        progress_pct = (i + 1) / total_downloads * 100
                        print(f"  Downloaded {downloaded_count}/{total_downloads} files ({progress_pct:.1f}%) - {elapsed_time/60:.1f}m elapsed")
                
            except Exception as e:
                print(f"  Error downloading {run_time_str}/{ensemble}/{step}: {e}")
                continue
    
    else:
        # Old format: dictionary structure
        # Calculate total downloads
        total_downloads = 0
        for run_time_str in run_ensemble_steps:
            ensembles = list(run_ensemble_steps[run_time_str].keys())
            if ensembles:
                first_ensemble = ensembles[0]
                steps = run_ensemble_steps[run_time_str][first_ensemble]
                total_downloads += len(ensembles) * len(steps)
        
        if max_files:
            total_downloads = min(total_downloads, max_files)
            print(f"Limiting download to first {max_files} files")
        
        print(f"Downloading {total_downloads} GRIB2 files...")
        print("This may take a while - downloading all ensemble members for all runs and steps...")
        
        for run_i, (run_time_str, ensembles_data) in enumerate(run_ensemble_steps.items(), 1):
            readable_time = run_time_str.replace('%3A', ':')
            ensembles = list(ensembles_data.keys())
            
            if not ensembles:
                continue
            
            first_ensemble = ensembles[0]
            steps = ensembles_data[first_ensemble]
            
            print(f"\nRun {run_i}/{len(run_ensemble_steps)}: {readable_time}")
            print(f"  Downloading {len(ensembles)} ensembles × {len(steps)} steps = {len(ensembles) * len(steps)} files")
            
            run_files = []
            ensemble_count = 0
            
            for ensemble in ensembles:
                ensemble_files = []
                for step in steps:
                    # Check if we've reached the file limit
                    if max_files and downloaded_count >= max_files:
                        break
                    
                    try:
                        filepath = download_grib_file(run_time_str, ensemble, step)
                        
                        if filepath and Path(filepath).exists():
                            ensemble_files.append(filepath)
                            downloaded_files.append(filepath)
                            downloaded_count += 1
                            
                            # Show progress periodically
                            if downloaded_count % progress_interval == 0:
                                elapsed = time.time() - start_time
                                rate = downloaded_count / elapsed if elapsed > 0 else 0
                                print(f"    Downloaded {downloaded_count}/{total_downloads} files ({rate:.1f} files/sec)")
                    
                    except Exception as e:
                        print(f"    Error downloading {run_time_str}/{ensemble}/{step}: {e}")
                        continue
                
                run_files.extend(ensemble_files)
                ensemble_count += 1
                
                # Show progress every 5 ensemble members or at the end
                if ensemble_count % 5 == 0 or ensemble_count == len(ensembles):
                    print(f"    Completed {ensemble_count}/{len(ensembles)} ensembles for this run")
                
                # Break if we've reached the file limit
                if max_files and downloaded_count >= max_files:
                    break
            
            print(f"  Completed run {readable_time}: {len(run_files)} files")
            
            # Break if we've reached the file limit
            if max_files and downloaded_count >= max_files:
                break

    elapsed = time.time() - start_time
    print(f"\nDownload complete:")
    print(f"  Files downloaded: {len(downloaded_files)}")
    print(f"  Time elapsed: {elapsed/60:.1f} minutes")
    if elapsed > 0:
        print(f"  Average rate: {len(downloaded_files)/elapsed:.1f} files/sec")
    
    if len(downloaded_files) < total_downloads:
        failed = total_downloads - len(downloaded_files)
        print(f"  ⚠️  {failed} files failed to download")
    
    return downloaded_files

def parallel_batch_download(run_ensemble_steps: Union[Dict, List[Tuple]], 
                          max_files: Optional[int] = None, 
                          max_workers: int = 10, 
                          download_dir: Optional[Path] = None) -> List[str]:
    """
    Download GRIB2 files in parallel using ThreadPoolExecutor.
    
    Args:
        run_ensemble_steps: Either a dict (from discovery) or list of tuples [(run_time, ensemble, step), ...]
        max_files: Maximum number of files to download (for testing)
        max_workers: Number of concurrent download threads (default: 10)
        download_dir: Directory to save files (default: RAW_DATA_DIR)
    
    Returns:
        list: List of successfully downloaded file paths
    """
    if not run_ensemble_steps:
        print("No runs found for download!")
        return []
    
    # Convert input to standardized format: list of tuples
    download_tasks = []
    
    if isinstance(run_ensemble_steps, list):
        # New format: list of (run_time, ensemble, step) tuples
        download_tasks = [(rt, ens, step, download_dir) for rt, ens, step in run_ensemble_steps]
        
    else:
        # Old format: dictionary structure
        for run_time_str, ensembles_data in run_ensemble_steps.items():
            ensembles = list(ensembles_data.keys())
            if not ensembles:
                continue
            first_ensemble = ensembles[0]
            steps = ensembles_data[first_ensemble]
            
            for ensemble in ensembles:
                for step in steps:
                    download_tasks.append((run_time_str, ensemble, step, download_dir))
    
    if max_files:
        download_tasks = download_tasks[:max_files]
        print(f"Limiting download to first {max_files} files")
    
    total_downloads = len(download_tasks)
    print(f"Starting parallel download of {total_downloads} GRIB2 files using {max_workers} workers...")
    
    downloaded_files = []
    failed_downloads = []
    start_time = time.time()
    
    # Use progress bar if tqdm is available
    if TQDM_AVAILABLE:
        progress_bar = tqdm(total=total_downloads, desc="Downloading", unit="files")
    else:
        progress_bar = None
    
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all download tasks
            future_to_task = {executor.submit(parallel_download_grib_file, task): task for task in download_tasks}
            
            # Process completed downloads
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                run_time_str, ensemble, step, _ = task
                
                try:
                    filepath = future.result()
                    if filepath and Path(filepath).exists():
                        downloaded_files.append(filepath)
                    else:
                        failed_downloads.append(f"{run_time_str}/{ensemble}/{step}")
                        
                except Exception as e:
                    print(f"Error downloading {run_time_str}/{ensemble}/{step}: {e}")
                    failed_downloads.append(f"{run_time_str}/{ensemble}/{step}")
                
                # Update progress
                if progress_bar:
                    progress_bar.update(1)
                else:
                    # Simple progress without tqdm
                    completed = len(downloaded_files) + len(failed_downloads)
                    if completed % 10 == 0:
                        elapsed = time.time() - start_time
                        rate = completed / elapsed if elapsed > 0 else 0
                        print(f"Progress: {completed}/{total_downloads} files ({rate:.1f} files/sec)")
    
    finally:
        if progress_bar:
            progress_bar.close()
    
    elapsed = time.time() - start_time
    success_count = len(downloaded_files)
    
    print(f"\nParallel download complete:")
    print(f"  Files downloaded: {success_count}/{total_downloads}")
    print(f"  Failed downloads: {len(failed_downloads)}")
    print(f"  Time elapsed: {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
    
    if elapsed > 0:
        print(f"  Average rate: {success_count/elapsed:.1f} files/sec")
    
    if failed_downloads:
        print(f"\nFailed downloads:")
        for failed in failed_downloads[:10]:  # Show first 10
            print(f"  {failed}")
        if len(failed_downloads) > 10:
            print(f"  ... and {len(failed_downloads) - 10} more")
    
    return downloaded_files

async def async_download_grib_file(session: aiohttp.ClientSession, 
                                 run_time_str: str, 
                                 ensemble: str, 
                                 step: str, 
                                 download_dir: Optional[Path] = None,
                                 semaphore: Optional[asyncio.Semaphore] = None) -> Optional[str]:
    """
    Asynchronously download a single GRIB file.
    
    Args:
        session: aiohttp client session
        run_time_str: Run time in URL format
        ensemble: Ensemble member number
        step: Forecast step
        download_dir: Directory to save the file
        semaphore: Semaphore to limit concurrent downloads
    
    Returns:
        str: Path to downloaded file, or None if failed
    """
    if semaphore:
        await semaphore.acquire()
    
    try:
        if download_dir is None:
            download_dir = RAW_DATA_DIR
        
        # Create download directory if it doesn't exist
        Path(download_dir).mkdir(parents=True, exist_ok=True)
        
        # Construct URL and filename
        url = BASE_DOWNLOAD_URL.format(run_time=run_time_str, ensemble=ensemble, step=step)
        readable_time = run_time_str.replace('%3A', ':')
        run_time_dt = datetime.strptime(readable_time, '%Y-%m-%dT%H:%M')
        filename = GRIB_FILENAME_PATTERN.format(
            run_date=run_time_dt.strftime('%Y%m%d'),
            run_hour=run_time_dt.strftime('%H'),
            ensemble=ensemble,
            step=step
        )
        filepath = Path(download_dir) / filename
        
        # Skip if file already exists
        if filepath.exists():
            return str(filepath)
        
        # Download file
        timeout = aiohttp.ClientTimeout(total=120)
        async with session.get(url, timeout=timeout) as response:
            if response.status == 200:
                with open(filepath, 'wb') as f:
                    async for chunk in response.content.iter_chunked(8192):
                        f.write(chunk)
                return str(filepath)
            else:
                print(f"HTTP {response.status} for {url}")
                return None
                
    except asyncio.TimeoutError:
        print(f"Timeout downloading {run_time_str}/{ensemble}/{step}")
        return None
    except Exception as e:
        print(f"Error downloading {run_time_str}/{ensemble}/{step}: {e}")
        return None
    finally:
        if semaphore:
            semaphore.release()

async def async_batch_download(run_ensemble_steps: Union[Dict, List[Tuple]], 
                             max_files: Optional[int] = None,
                             max_concurrent: int = 20,
                             download_dir: Optional[Path] = None) -> List[str]:
    """
    Download GRIB2 files asynchronously using aiohttp.
    
    Args:
        run_ensemble_steps: Either a dict or list of tuples [(run_time, ensemble, step), ...]
        max_files: Maximum number of files to download
        max_concurrent: Maximum concurrent downloads (default: 20)
        download_dir: Directory to save files
    
    Returns:
        list: List of successfully downloaded file paths
    """
    if not run_ensemble_steps:
        print("No runs found for download!")
        return []
    
    # Convert input to standardized format
    download_tasks = []
    
    if isinstance(run_ensemble_steps, list):
        download_tasks = [(rt, ens, step) for rt, ens, step in run_ensemble_steps]
    else:
        # Old format: dictionary structure
        for run_time_str, ensembles_data in run_ensemble_steps.items():
            ensembles = list(ensembles_data.keys())
            if not ensembles:
                continue
            first_ensemble = ensembles[0]
            steps = ensembles_data[first_ensemble]
            
            for ensemble in ensembles:
                for step in steps:
                    download_tasks.append((run_time_str, ensemble, step))
    
    if max_files:
        download_tasks = download_tasks[:max_files]
        print(f"Limiting download to first {max_files} files")
    
    total_downloads = len(download_tasks)
    print(f"Starting async download of {total_downloads} GRIB2 files with max {max_concurrent} concurrent...")
    
    start_time = time.time()
    downloaded_files = []
    
    # Create semaphore to limit concurrent downloads
    semaphore = asyncio.Semaphore(max_concurrent)
    
    # Configure aiohttp session with connection pooling
    connector = aiohttp.TCPConnector(
        limit=max_concurrent + 10,
        limit_per_host=max_concurrent,
        ttl_dns_cache=300,
        use_dns_cache=True
    )
    
    timeout = aiohttp.ClientTimeout(total=300, connect=30)
    
    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        headers={'User-Agent': 'ICON-RUC-EPS-Downloader/1.0'}
    ) as session:
        
        # Create all download tasks
        tasks = [
            async_download_grib_file(session, rt, ens, step, download_dir, semaphore)
            for rt, ens, step in download_tasks
        ]
        
        # Use progress tracking if tqdm is available
        if TQDM_AVAILABLE:
            progress_bar = tqdm(total=total_downloads, desc="Async Downloading", unit="files")
        
        # Process downloads as they complete
        completed = 0
        failed = 0
        
        for task in asyncio.as_completed(tasks):
            result = await task
            completed += 1
            
            if result:
                downloaded_files.append(result)
            else:
                failed += 1
            
            # Update progress
            if TQDM_AVAILABLE:
                progress_bar.update(1)
            elif completed % 20 == 0:
                elapsed = time.time() - start_time
                rate = completed / elapsed if elapsed > 0 else 0
                print(f"Progress: {completed}/{total_downloads} files ({rate:.1f} files/sec)")
        
        if TQDM_AVAILABLE:
            progress_bar.close()
    
    elapsed = time.time() - start_time
    success_count = len(downloaded_files)
    
    print(f"\nAsync download complete:")
    print(f"  Files downloaded: {success_count}/{total_downloads}")
    print(f"  Failed downloads: {failed}")
    print(f"  Time elapsed: {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
    
    if elapsed > 0:
        print(f"  Average rate: {success_count/elapsed:.1f} files/sec")
    
    return downloaded_files

def run_async_download(run_ensemble_steps: Union[Dict, List[Tuple]], 
                      max_files: Optional[int] = None,
                      max_concurrent: int = 20,
                      download_dir: Optional[Path] = None) -> List[str]:
    """
    Convenience function to run async download from synchronous code.
    
    Args:
        run_ensemble_steps: Either a dict or list of tuples
        max_files: Maximum number of files to download
        max_concurrent: Maximum concurrent downloads
        download_dir: Directory to save files
    
    Returns:
        list: List of successfully downloaded file paths
    """
    import asyncio
    
    # Enable nested event loops for Jupyter notebooks
    try:
        import nest_asyncio
        nest_asyncio.apply()
    except ImportError:
        pass  # nest_asyncio not available, will handle gracefully
    
    try:
        # Check if we're in an existing event loop (like Jupyter)
        try:
            loop = asyncio.get_running_loop()
            # We're in a running event loop (Jupyter) - use different approach
            print("   Detected Jupyter environment - using compatible async method")
            
            # Create a new event loop in a thread for true async execution
            import concurrent.futures
            import threading
            
            def run_in_thread():
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)
                try:
                    return new_loop.run_until_complete(
                        async_batch_download(run_ensemble_steps, max_files, max_concurrent, download_dir)
                    )
                finally:
                    new_loop.close()
            
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(run_in_thread)
                return future.result()
                
        except RuntimeError:
            # No running event loop - we can use asyncio.run normally
            return asyncio.run(async_batch_download(run_ensemble_steps, max_files, max_concurrent, download_dir))
            
    except Exception as e:
        print(f"   Async execution failed: {e}")
        print(f"   Falling back to parallel method...")
        # Fallback to parallel download
        return parallel_batch_download(
            run_ensemble_steps, 
            max_files=max_files, 
            max_workers=min(max_concurrent, 10)  # Limit workers for stability
        )

def smart_batch_download(run_ensemble_steps: Union[Dict, List[Tuple]], 
                        max_files: Optional[int] = None,
                        method: str = "parallel",
                        max_workers: int = 10,
                        max_concurrent: int = 20,
                        download_dir: Optional[Path] = None) -> List[str]:
    """
    Smart download function that chooses the best method based on requirements.
    
    Args:
        run_ensemble_steps: Either a dict or list of tuples
        max_files: Maximum number of files to download
        method: "sequential", "parallel", or "async" (default: "parallel")
        max_workers: Number of threads for parallel method
        max_concurrent: Number of concurrent connections for async method
        download_dir: Directory to save files
    
    Returns:
        list: List of successfully downloaded file paths
    """
    if not run_ensemble_steps:
        print("No runs found for download!")
        return []
    
    # Auto-select method based on number of files if not specified
    if method == "auto":
        if isinstance(run_ensemble_steps, list):
            total_files = len(run_ensemble_steps)
        else:
            total_files = sum(len(list(ensembles.keys())) * len(list(ensembles.values())[0]) 
                            if ensembles else 0
                            for ensembles in run_ensemble_steps.values())
        
        if total_files < 10:
            method = "sequential"
        elif total_files < 100:
            method = "parallel"
        else:
            method = "async"
        
        print(f"Auto-selected {method} method for {total_files} files")
    
    if method == "sequential":
        return batch_download(run_ensemble_steps, max_files, 10)  # Original function with progress
    elif method == "parallel":
        return parallel_batch_download(run_ensemble_steps, max_files, max_workers, download_dir)
    elif method == "async":
        return run_async_download(run_ensemble_steps, max_files, max_concurrent, download_dir)
    else:
        raise ValueError(f"Unknown download method: {method}. Use 'sequential', 'parallel', 'async', or 'auto'")

def verify_downloads(file_list, min_size_kb=1):
    """
    Verify that downloaded files exist and have reasonable sizes.
    
    Args:
        file_list (list): List of file paths to verify
        min_size_kb (int): Minimum file size in KB
    
    Returns:
        dict: Verification results
    """
    results = {
        'total_files': len(file_list),
        'valid_files': [],
        'missing_files': [],
        'small_files': [],
        'total_size_mb': 0
    }
    
    for filepath in file_list:
        path = Path(filepath)
        
        if not path.exists():
            results['missing_files'].append(str(filepath))
            continue
        
        size_bytes = path.stat().st_size
        size_kb = size_bytes / 1024
        
        if size_kb < min_size_kb:
            results['small_files'].append({
                'file': str(filepath),
                'size_kb': size_kb
            })
        else:
            results['valid_files'].append(str(filepath))
            results['total_size_mb'] += size_bytes / (1024 * 1024)
    
    return results

def print_download_summary(file_list):
    """
    Print a summary of downloaded files.
    
    Args:
        file_list (list): List of downloaded file paths
    """
    if not file_list:
        print("No files to summarize")
        return
    
    verification = verify_downloads(file_list)
    
    print(f"\nDOWNLOAD SUMMARY")
    print(f"================")
    print(f"Total files: {verification['total_files']}")
    print(f"Valid files: {len(verification['valid_files'])}")
    print(f"Missing files: {len(verification['missing_files'])}")
    print(f"Small files: {len(verification['small_files'])}")
    print(f"Total size: {verification['total_size_mb']:.1f} MB")
    
    if verification['missing_files']:
        print(f"\nMissing files:")
        for file in verification['missing_files'][:5]:  # Show first 5
            print(f"  {file}")
        if len(verification['missing_files']) > 5:
            print(f"  ... and {len(verification['missing_files']) - 5} more")
    
    if verification['small_files']:
        print(f"\nSuspiciously small files:")
        for file_info in verification['small_files'][:5]:  # Show first 5
            print(f"  {file_info['file']} ({file_info['size_kb']:.1f} KB)")
        if len(verification['small_files']) > 5:
            print(f"  ... and {len(verification['small_files']) - 5} more")

def estimate_download_size(run_ensemble_steps, avg_file_size_mb=8):
    """
    Estimate total download size based on discovered data.
    
    Args:
        run_ensemble_steps (dict): Discovery results
        avg_file_size_mb (float): Average GRIB2 file size in MB
    
    Returns:
        dict: Size estimates
    """
    total_files = 0
    
    for run_time_str in run_ensemble_steps:
        ensembles = list(run_ensemble_steps[run_time_str].keys())
        if ensembles:
            first_ensemble = ensembles[0]
            steps = run_ensemble_steps[run_time_str][first_ensemble]
            total_files += len(ensembles) * len(steps)
    
    total_size_mb = total_files * avg_file_size_mb
    total_size_gb = total_size_mb / 1024
    
    return {
        'total_files': total_files,
        'estimated_size_mb': total_size_mb,
        'estimated_size_gb': total_size_gb
    }

def clean_old_downloads(days_old=7, dry_run=True):
    """
    Clean up old downloaded files.
    
    Args:
        days_old (int): Delete files older than this many days
        dry_run (bool): If True, only print what would be deleted
    
    Returns:
        int: Number of files deleted (or would be deleted)
    """
    if not RAW_DATA_DIR.exists():
        print(f"Download directory does not exist: {RAW_DATA_DIR}")
        return 0
    
    cutoff_time = datetime.now().timestamp() - (days_old * 24 * 3600)
    files_to_delete = []
    
    for file_path in RAW_DATA_DIR.glob("*.grib2"):
        if file_path.stat().st_mtime < cutoff_time:
            files_to_delete.append(file_path)
    
    if not files_to_delete:
        print(f"No files older than {days_old} days found")
        return 0
    
    total_size = sum(f.stat().st_size for f in files_to_delete) / (1024 * 1024)
    
    if dry_run:
        print(f"Would delete {len(files_to_delete)} files ({total_size:.1f} MB)")
        for f in files_to_delete[:5]:  # Show first 5
            age_days = (datetime.now().timestamp() - f.stat().st_mtime) / (24 * 3600)
            print(f"  {f.name} (age: {age_days:.1f} days)")
        if len(files_to_delete) > 5:
            print(f"  ... and {len(files_to_delete) - 5} more")
    else:
        print(f"Deleting {len(files_to_delete)} files ({total_size:.1f} MB)...")
        for file_path in files_to_delete:
            file_path.unlink()
        print("Cleanup completed")
    
    return len(files_to_delete)

def benchmark_download_methods(run_ensemble_steps: Union[Dict, List[Tuple]], 
                              max_files: int = 20) -> Dict[str, float]:
    """
    Benchmark different download methods to compare performance.
    
    Args:
        run_ensemble_steps: Download tasks
        max_files: Number of files to test with
    
    Returns:
        dict: Performance results for each method
    """
    if not run_ensemble_steps:
        print("No data to benchmark!")
        return {}
    
    # Ensure we have a reasonable test size
    test_size = min(max_files, 20)
    print(f"Benchmarking download methods with {test_size} files...")
    print("=" * 60)
    
    results = {}
    
    # Test sequential method (original)
    print("\n🐌 Testing Sequential Download:")
    start_time = time.time()
    seq_files = batch_download(run_ensemble_steps, test_size, 5)
    seq_time = time.time() - start_time
    results['sequential'] = {
        'time': seq_time,
        'files': len(seq_files),
        'rate': len(seq_files) / seq_time if seq_time > 0 else 0
    }
    print(f"Sequential: {len(seq_files)} files in {seq_time:.1f}s ({results['sequential']['rate']:.1f} files/sec)")
    
    # Test parallel method
    print("\n🚀 Testing Parallel Download (ThreadPoolExecutor):")
    start_time = time.time()
    par_files = parallel_batch_download(run_ensemble_steps, test_size, 10)
    par_time = time.time() - start_time
    results['parallel'] = {
        'time': par_time,
        'files': len(par_files),
        'rate': len(par_files) / par_time if par_time > 0 else 0
    }
    print(f"Parallel: {len(par_files)} files in {par_time:.1f}s ({results['parallel']['rate']:.1f} files/sec)")
    
    # Test async method
    print("\n⚡ Testing Async Download (aiohttp):")
    start_time = time.time()
    async_files = run_async_download(run_ensemble_steps, test_size, 15)
    async_time = time.time() - start_time
    results['async'] = {
        'time': async_time,
        'files': len(async_files),
        'rate': len(async_files) / async_time if async_time > 0 else 0
    }
    print(f"Async: {len(async_files)} files in {async_time:.1f}s ({results['async']['rate']:.1f} files/sec)")
    
    # Performance comparison
    print("\n" + "=" * 60)
    print("📊 PERFORMANCE COMPARISON:")
    print("=" * 60)
    
    if results['sequential']['time'] > 0:
        par_speedup = results['parallel']['rate'] / results['sequential']['rate']
        async_speedup = results['async']['rate'] / results['sequential']['rate']
        
        print(f"Sequential:  {results['sequential']['rate']:.1f} files/sec (baseline)")
        print(f"Parallel:    {results['parallel']['rate']:.1f} files/sec ({par_speedup:.1f}x faster)")
        print(f"Async:       {results['async']['rate']:.1f} files/sec ({async_speedup:.1f}x faster)")
        
        fastest = max(results.keys(), key=lambda k: results[k]['rate'])
        print(f"\n🏆 Fastest method: {fastest.upper()}")
    
    return results

# Usage Examples
def usage_examples():
    """
    Print usage examples for the optimized download functions.
    """
    print("""
=== ICON-RUC-EPS Download Optimization Usage Examples ===

1. BASIC PARALLEL DOWNLOAD (Recommended):
   from utils.download import parallel_batch_download
   
   # Download with 10 concurrent threads
   files = parallel_batch_download(run_ensemble_steps, max_workers=10)

2. ASYNC DOWNLOAD (Best for many files):
   from utils.download import run_async_download
   
   # Download with 20 concurrent connections
   files = run_async_download(run_ensemble_steps, max_concurrent=20)

3. SMART AUTO-SELECTION:
   from utils.download import smart_batch_download
   
   # Automatically chooses best method based on file count
   files = smart_batch_download(run_ensemble_steps, method="auto")

4. PERFORMANCE BENCHMARKING:
   from utils.download import benchmark_download_methods
   
   # Compare all methods with 20 test files
   results = benchmark_download_methods(run_ensemble_steps, max_files=20)

5. BACKWARD COMPATIBILITY:
   from utils.download import batch_download  # Original sequential function
   
   # Still works exactly as before
   files = batch_download(run_ensemble_steps, max_files=50)

=== PERFORMANCE TIPS ===

• Use parallel_batch_download() for 10-100 files (10-20x faster)
• Use run_async_download() for 100+ files (15-30x faster)  
• Install tqdm for progress bars: pip install tqdm aiohttp
• Adjust max_workers/max_concurrent based on your network and system
• Files are automatically skipped if they already exist

=== METHOD COMPARISON ===

Sequential (original): ~1-2 files/sec, low resource usage
Parallel (threads):    ~10-20 files/sec, moderate CPU/memory  
Async (aiohttp):       ~15-30 files/sec, low CPU, high network efficiency

Choose based on your needs:
- Few files (<10): Sequential is fine
- Moderate load (10-100): Parallel is optimal  
- Heavy load (100+): Async is best
    """)

if __name__ == "__main__":
    usage_examples()