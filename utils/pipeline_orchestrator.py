#!/usr/bin/env python3
"""
Pipeline Orchestrator
====================

Main orchestration logic for the modular weather pipeline.
"""

import asyncio
import concurrent.futures
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import logging
import ssl
import certifi
import re
from collections import defaultdict

from .models import (ForecastRun, VariableData, EnsembleMember, TargetLocation,
                    ProcessingConfig, get_variable_config, GridInfo)
from .extraction import (extract_point_from_grib, validate_extraction_setup)
from .statistics import calculate_ensemble_statistics, validate_ensemble_data
from .data_io import save_all_outputs, OutputManager
# Import existing utilities from the current codebase
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from utils.discovery import discover_all_data, get_available_ensembles, get_available_steps
from utils.download import smart_batch_download
from utils.grid import download_icon_grid_definition
from scipy.spatial import KDTree
import numpy as np
from config import RAW_DATA_DIR

logger = logging.getLogger(__name__)


def find_existing_grib_files(raw_dir: Optional[Path] = None) -> Dict[str, Dict[str, List[str]]]:
    """
    Find existing GRIB files in the raw data directory and group them by run time and variable.

    Args:
        raw_dir: Directory containing raw GRIB files (default: RAW_DATA_DIR from config)

    Returns:
        Dict mapping run_time_str -> {variable_id -> [list of file paths]}
        Example: {
            '2025-10-09T13:00': {
                'TOT_PREC': ['/path/to/file1.grib2', '/path/to/file2.grib2', ...],
                'VMAX_10M': ['/path/to/file3.grib2', ...]
            }
        }
    """
    if raw_dir is None:
        raw_dir = RAW_DATA_DIR

    if not raw_dir.exists():
        logger.warning(f"Raw data directory does not exist: {raw_dir}")
        return {}

    # Pattern: icon_d2_ruc_eps_{VARIABLE}_{YYYYMMDDTHHMM}_e{NN}_PT{HHH}H{MM}M.grib2
    # Example: icon_d2_ruc_eps_TOT_PREC_2025-10-09T1300_e01_PT000H05M.grib2
    pattern = re.compile(
        r'icon_d2_ruc_eps_(?P<variable>[A-Z_]+)_'
        r'(?P<date>\d{4}-\d{2}-\d{2})T(?P<time>\d{4})_'
        r'e(?P<ensemble>\d+)_'
        r'PT(?P<step>.+)\.grib2'
    )

    # Group files by run time and variable
    runs_data = defaultdict(lambda: defaultdict(list))

    grib_files = list(raw_dir.glob("icon_d2_ruc_eps_*.grib2"))
    logger.info(f"Found {len(grib_files)} GRIB files in {raw_dir}")

    for grib_file in grib_files:
        match = pattern.match(grib_file.name)
        if not match:
            logger.debug(f"Skipping file with unrecognized pattern: {grib_file.name}")
            continue

        variable = match.group('variable')
        date = match.group('date')
        time_str = match.group('time')  # e.g., "1300"

        # Convert time to HH:MM format
        if len(time_str) == 4:
            hour = time_str[:2]
            minute = time_str[2:]
            run_time_str = f"{date}T{hour}:{minute}"
        else:
            logger.warning(f"Unexpected time format in {grib_file.name}: {time_str}")
            continue

        runs_data[run_time_str][variable].append(str(grib_file))

    # Convert defaultdict to regular dict and sort files
    result = {}
    for run_time, variables in runs_data.items():
        result[run_time] = {}
        for var_id, files in variables.items():
            result[run_time][var_id] = sorted(files)

    logger.info(f"Found data for {len(result)} forecast runs")
    for run_time, variables in result.items():
        var_summary = ", ".join([f"{var}: {len(files)} files" for var, files in variables.items()])
        logger.info(f"  {run_time}: {var_summary}")

    return result


class PipelineOrchestrator:
    """Main orchestrator for the modular weather pipeline"""
    
    def __init__(self, config: ProcessingConfig):
        self.config = config
        self.grid_info = None
        self.target_location = None
        
    def initialize_grid(self, cache_dir: Optional[Path] = None) -> GridInfo:
        """Initialize ICON grid information"""
        logger.info("Initializing ICON grid...")
        
        # Download and load grid definition (returns lat/lon arrays directly)
        grid_result = download_icon_grid_definition(cache_dir)
        if not grid_result or len(grid_result) != 2:
            raise RuntimeError("Failed to download ICON grid definition")
        
        lats, lons = grid_result
        
        # Convert masked arrays to regular numpy arrays
        lats = np.array(lats)
        lons = np.array(lons)
        
        logger.info(f"Loaded grid with {len(lats)} points")
        logger.info(f"Lat range: {np.min(lats):.3f}° to {np.max(lats):.3f}°")
        logger.info(f"Lon range: {np.min(lons):.3f}° to {np.max(lons):.3f}°")
        
        # Build KDTree for efficient spatial queries
        logger.info("Building spatial index...")
        grid_coords_rad = np.column_stack([
            np.radians(lats.flatten()),
            np.radians(lons.flatten())
        ])
        kdtree = KDTree(grid_coords_rad)
        
        self.grid_info = GridInfo(
            lats=lats,
            lons=lons,
            kdtree=kdtree,
            metadata={
                'source': 'download_icon_grid_definition',
                'num_points': len(lats),
                'lat_range': [float(np.min(lats)), float(np.max(lats))],
                'lon_range': [float(np.min(lons)), float(np.max(lons))]
            }
        )
        
        logger.info(f"Grid initialized with {len(lats)} points")
        return self.grid_info
    
    def setup_target_location(self, lat: float, lon: float, name: str = "Target") -> TargetLocation:
        """Setup target location for extraction"""
        if not self.grid_info:
            raise RuntimeError("Grid not initialized - call initialize_grid() first")
        
        self.target_location = TargetLocation(lat=lat, lon=lon, name=name)
        
        # Validate and find nearest grid point
        if not validate_extraction_setup(self.target_location, self.grid_info, 
                                        get_variable_config('TOT_PREC')):
            raise ValueError("Invalid target location or grid setup")
        
        # Pre-compute grid index for efficiency
        from .extraction import find_nearest_grid_point
        grid_index, distance = find_nearest_grid_point(
            lat, lon, self.grid_info.lats, self.grid_info.lons, self.grid_info.kdtree
        )
        
        self.target_location.grid_index = grid_index
        self.target_location.distance_km = distance
        
        logger.info(f"Target location: {name} ({lat:.4f}°N, {lon:.4f}°E)")
        logger.info(f"Nearest grid point distance: {distance:.2f} km")
        
        return self.target_location
    
    async def discover_forecast_runs(self) -> List[Dict[str, Any]]:
        """Discover available forecast runs"""
        logger.info(f"Discovering {self.config.num_runs} latest forecast runs...")
        
        # Use existing discovery utilities
        discovery_results = discover_all_data(num_runs=self.config.num_runs)
        
        if not discovery_results:
            raise RuntimeError("No forecast runs discovered")
        
        # Convert discovery results to list of run info dicts
        filtered_runs = []
        for run_time_str, ensemble_data in discovery_results.items():
            # Check if run has all requested variables (simplified check)
            has_all_variables = True
            for var_id in self.config.variables:
                if var_id not in ['TOT_PREC', 'VMAX_10M']:  # Known supported variables
                    has_all_variables = False
                    break
            
            if has_all_variables:
                run_info = {
                    'run_time': run_time_str.replace('%3A', ':'),  # URL decode
                    'ensembles': list(ensemble_data.keys()),
                    'num_ensembles': len(ensemble_data)
                }
                filtered_runs.append(run_info)
        
        logger.info(f"Found {len(filtered_runs)} suitable forecast runs")
        return filtered_runs[:self.config.num_runs]
    
    async def download_run_data(self, run_info: Dict[str, Any]) -> Dict[str, List[str]]:
        """Download GRIB files for a forecast run"""
        run_time_str = run_info['run_time']
        logger.info(f"Downloading data for run {run_time_str}")

        # Get ensemble information
        ensembles = get_available_ensembles(run_time_str)
        if not ensembles:
            raise RuntimeError(f"No ensembles found for run {run_time_str}")

        # Limit ensembles if needed (for testing)
        max_ensembles = 20  # Full ensemble
        ensembles = ensembles[:max_ensembles]

        # Download files for each variable separately with variable-specific URLs and steps
        files_by_variable = {}

        for var_id in self.config.variables:
            logger.info(f"Downloading {var_id} files...")

            # Get variable-specific forecast steps
            steps = get_available_steps(run_time_str, ensembles[0], variable_id=var_id)
            if not steps:
                logger.error(f"No forecast steps found for {var_id} in run {run_time_str}")
                files_by_variable[var_id] = []
                continue

            # Prepare download list for this variable
            var_download_list = []
            for ensemble in ensembles:
                for step in steps:
                    var_download_list.append((run_time_str, ensemble, step))

            logger.info(f"Downloading {len(var_download_list)} files for {var_id} "
                       f"({len(ensembles)} ens × {len(steps)} steps)")

            try:
                # Use variable-specific download
                downloaded_files = await self._download_variable_files(
                    var_id, var_download_list,
                    max_workers=min(self.config.max_workers, 4)
                )

                # Filter for actual files that contain the variable
                var_files = [f for f in downloaded_files
                           if f and Path(f).exists()]

                files_by_variable[var_id] = var_files
                logger.info(f"Downloaded {len(var_files)} files for {var_id}")

            except Exception as e:
                logger.error(f"Error downloading {var_id}: {e}")
                files_by_variable[var_id] = []

        return files_by_variable
    
    async def _download_variable_files(self, var_id: str, 
                                     var_download_list: List[Tuple[str, str, str]],
                                     max_workers: int = 4) -> List[str]:
        """Download files for a specific variable"""
        import aiohttp
        import aiofiles
        import asyncio
        from pathlib import Path
        
        # Get variable configuration
        from config import VARIABLES_CONFIG
        var_config = VARIABLES_CONFIG[var_id]
        
        # Base URL for this variable
        base_download_url = f"{var_config['base_url']}r/{{run_time}}/e/{{ensemble}}/s/{{step}}"
        
        # Create download directory
        download_dir = Path("data/raw")
        download_dir.mkdir(parents=True, exist_ok=True)
        
        # Semaphore to limit concurrent downloads
        semaphore = asyncio.Semaphore(max_workers)
        
        async def download_single_file(session: aiohttp.ClientSession, 
                                     run_time_str: str, ensemble: str, step: str) -> Optional[str]:
            """Download a single GRIB file"""
            async with semaphore:
                try:
                    # Construct download URL for this variable
                    url = base_download_url.format(
                        run_time=run_time_str.replace(':', '%3A'),
                        ensemble=ensemble,
                        step=step
                    )
                    
                    # Construct filename (same format as original system)
                    filename = f"icon_d2_ruc_eps_{var_id}_{run_time_str.replace(':', '')}_e{ensemble}_{step}"
                    filepath = download_dir / filename
                    
                    # Skip if file already exists
                    if filepath.exists():
                        logger.debug(f"File already exists: {filename}")
                        return str(filepath)
                    
                    logger.debug(f"Downloading: {url}")
                    
                    # Download file
                    async with session.get(url) as response:
                        if response.status == 200:
                            async with aiofiles.open(filepath, 'wb') as f:
                                async for chunk in response.content.iter_chunked(8192):
                                    await f.write(chunk)
                            logger.debug(f"Successfully downloaded: {filename}")
                            return str(filepath)
                        else:
                            logger.error(f"Failed to download {url}: HTTP {response.status}")
                            return None
                            
                except Exception as e:
                    logger.error(f"Error downloading {url}: {e}")
                    return None
        
        # Download all files
        downloaded_files = []

        # Create SSL context with certifi certificates
        ssl_context = ssl.create_default_context(cafile=certifi.where())

        connector = aiohttp.TCPConnector(
            limit=max_workers,
            limit_per_host=max_workers,
            ssl=ssl_context
        )

        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = []
            for run_time_str, ensemble, step in var_download_list:
                task = download_single_file(session, run_time_str, ensemble, step)
                tasks.append(task)
            
            logger.info(f"Starting download of {len(tasks)} files for {var_id}...")
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, str):  # Successful download
                    downloaded_files.append(result)
                elif isinstance(result, Exception):
                    logger.error(f"Download task failed: {result}")
        
        return downloaded_files
    
    def process_ensemble_data(self, grib_files: List[str], 
                            variable_id: str, run_time: str = None) -> List[EnsembleMember]:
        """Process GRIB files to extract ensemble data"""
        variable = get_variable_config(variable_id)
        logger.info(f"Processing {len(grib_files)} files for {variable_id}")
        
        # Set current run time for timestamp generation
        self.current_run_time = run_time
        
        # Group files by ensemble
        ensemble_files = {}
        for grib_file in grib_files:
            # Extract ensemble ID from filename
            import re
            # Pattern matches both new format (_e01_) and old format (ensemble_01)
            ens_match = re.search(r'_e(\d+)_', str(grib_file))
            if not ens_match:
                ens_match = re.search(r'ensemble_(\d+)', str(grib_file))
            
            if ens_match:
                ens_id = f"{int(ens_match.group(1)):02d}"
                if ens_id not in ensemble_files:
                    ensemble_files[ens_id] = []
                ensemble_files[ens_id].append(grib_file)
            else:
                logger.warning(f"Could not parse ensemble ID from filename: {Path(grib_file).name}")
        
        logger.info(f"Found {len(ensemble_files)} ensembles for {variable_id}")
        
        # Process each ensemble
        ensembles = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            # Submit jobs
            future_to_ensemble = {
                executor.submit(self._process_single_ensemble, ens_id, files, variable): ens_id
                for ens_id, files in ensemble_files.items()
            }
            
            # Collect results
            for future in concurrent.futures.as_completed(future_to_ensemble):
                ens_id = future_to_ensemble[future]
                try:
                    ensemble_data = future.result()
                    if ensemble_data:
                        ensembles.append(ensemble_data)
                except Exception as e:
                    logger.error(f"Error processing ensemble {ens_id}: {e}")
        
        logger.info(f"Successfully processed {len(ensembles)} ensembles for {variable_id}")
        return ensembles
    
    def _process_single_ensemble(self, ensemble_id: str, grib_files: List[str],
                               variable: Any) -> Optional[EnsembleMember]:
        """Process a single ensemble member"""
        logger.info(f"Processing ensemble {ensemble_id} with {len(grib_files)} files")
        
        try:
            # Sort files by step/time
            sorted_files = sorted(grib_files, key=lambda f: str(f))
            logger.debug(f"Ensemble {ensemble_id} sorted files: {[Path(f).name for f in sorted_files[:3]]}...")
            
            times = []
            values = []
            
            for i, grib_file in enumerate(sorted_files):
                logger.debug(f"Processing file {i+1}/{len(sorted_files)}: {Path(grib_file).name}")
                
                # Check if file exists
                if not Path(grib_file).exists():
                    logger.error(f"GRIB file does not exist: {grib_file}")
                    continue
                
                # Extract time information from file
                import re
                # Pattern matches both new format (PT000H05M.grib2) and old format (step_00005.grib2)  
                step_match = re.search(r'PT(\d{3})H(\d{2})M\.?g?r?i?b?2?', str(grib_file))
                if step_match:
                    hours = int(step_match.group(1))
                    minutes = int(step_match.group(2))
                    total_minutes = hours * 60 + minutes
                    logger.debug(f"Parsed step: PT{hours:03d}H{minutes:02d}M -> {total_minutes} minutes")
                else:
                    # Fallback to old format
                    step_match = re.search(r'step_(\d{5})\.grib2', str(grib_file))
                    if step_match:
                        step_str = step_match.group(1)
                        hours = int(step_str[:3])
                        minutes = int(step_str[3:])
                        total_minutes = hours * 60 + minutes
                        logger.debug(f"Parsed step (legacy): {step_str} -> {total_minutes} minutes")
                    else:
                        logger.warning(f"Could not parse step from {grib_file}")
                        continue
                
                # Extract value
                logger.debug(f"Extracting value from {Path(grib_file).name}...")
                value = extract_point_from_grib(
                    str(grib_file), self.target_location, self.grid_info, variable,
                    extraction_method=self.config.extraction_method,
                    neighbor_radius_km=self.config.neighbor_radius_km,
                    weighting_scheme=self.config.weighting_scheme
                )
                
                if value is not None:
                    # Convert minutes to ISO timestamp relative to actual run time
                    from datetime import datetime, timedelta
                    try:
                        # Parse the actual run time (format: 2025-08-31T06:00)
                        run_datetime = datetime.fromisoformat(self.current_run_time.replace('T', 'T').replace('%3A', ':'))
                        forecast_time = run_datetime + timedelta(minutes=total_minutes)
                        time_iso = forecast_time.strftime('%Y-%m-%dT_%H%M')
                    except:
                        # Fallback to placeholder if parsing fails
                        time_iso = f"2025-01-01T_{total_minutes//60:02d}{total_minutes%60:02d}"
                    
                    times.append(time_iso)
                    values.append(value)
                    logger.debug(f"Successfully extracted value: {value:.6f}")
                else:
                    logger.warning(f"Failed to extract value from {Path(grib_file).name}")
            
            logger.info(f"Ensemble {ensemble_id}: extracted {len(values)} values from {len(sorted_files)} files")

            if not times:
                logger.warning(f"No data extracted for ensemble {ensemble_id}")
                return None

            # Keep all timestamps - no cropping needed
            # TOT_PREC deaccumulation will handle first timestamp correctly in statistics
            # VMAX_10M should keep all values as they are instantaneous measurements

            ensemble_member = EnsembleMember(
                ensemble_id=ensemble_id,
                times=times,
                values=values,
                metadata={'num_files_processed': len(sorted_files)}
            )
            
            logger.info(f"Created ensemble member {ensemble_id} with {len(times)} time steps")
            return ensemble_member
            
        except Exception as e:
            logger.error(f"Error processing ensemble {ensemble_id}: {e}")
            import traceback
            logger.debug(f"Full traceback: {traceback.format_exc()}")
            return None
    
    async def process_forecast_run(self, run_info: Dict[str, Any]) -> ForecastRun:
        """Process a complete forecast run"""
        run_time_str = run_info['run_time']
        logger.info(f"Processing forecast run {run_time_str}")
        
        # Download data
        files_by_variable = await self.download_run_data(run_info)
        
        # Create forecast run object
        forecast_run = ForecastRun(
            run_time=run_time_str,
            location=self.target_location,
            processed_at=datetime.utcnow().isoformat()
        )
        
        # Process each variable
        for var_id in self.config.variables:
            if var_id not in files_by_variable or not files_by_variable[var_id]:
                logger.warning(f"No files found for variable {var_id}")
                continue
            
            logger.info(f"Processing variable {var_id}")
            
            # Extract ensemble data
            ensembles = self.process_ensemble_data(files_by_variable[var_id], var_id, run_time_str)
            
            if not ensembles:
                logger.warning(f"No ensemble data extracted for {var_id}")
                continue
            
            # Validate ensemble data
            validation_errors = validate_ensemble_data(ensembles)
            if validation_errors:
                logger.warning(f"Validation issues for {var_id}: {validation_errors}")
            
            # Calculate statistics
            variable = get_variable_config(var_id)
            statistics = calculate_ensemble_statistics(ensembles, variable)
            
            # Create variable data
            var_data = VariableData(
                variable=variable,
                ensembles=ensembles,
                statistics=statistics
            )
            
            forecast_run.variables[var_id] = var_data
            logger.info(f"Completed processing {var_id} with {len(ensembles)} ensembles")
        
        logger.info(f"Forecast run processing completed for {run_time_str}")
        return forecast_run
    
    async def process_forecast_run_from_files(self, run_info: Dict[str, Any], 
                                            files_by_variable: Dict[str, List[str]]) -> Optional[ForecastRun]:
        """Process a forecast run using existing files"""
        run_time_str = run_info['run_time']
        logger.info(f"Processing forecast run {run_time_str} from existing files")
        
        # Extract and process data for each variable
        variables_data = {}
        
        for var_id in self.config.variables:
            if var_id not in files_by_variable:
                logger.warning(f"No files found for variable {var_id}")
                continue
                
            grib_files = files_by_variable[var_id]
            logger.info(f"Processing {len(grib_files)} {var_id} files")
            
            # Process ensemble data
            ensembles = self.process_ensemble_data(grib_files, var_id, run_time_str)
            
            if ensembles:
                # Calculate statistics
                variable = get_variable_config(var_id)
                statistics = calculate_ensemble_statistics(ensembles, variable)
                
                variables_data[var_id] = VariableData(
                    variable=variable,
                    ensembles=ensembles,
                    statistics=statistics
                )
                logger.info(f"Successfully processed {len(ensembles)} ensembles for {var_id}")
            else:
                logger.warning(f"No ensemble data extracted for {var_id}")
        
        if not variables_data:
            logger.error("No data extracted for any variable")
            return None
        
        # Create forecast run
        forecast_run = ForecastRun(
            run_time=run_time_str,
            target_location=self.target_location,
            variables=variables_data
        )
        
        return forecast_run
    
    async def run_pipeline(self,
                          target_lat: float,
                          target_lon: float,
                          target_name: str = "Target Location",
                          output_dir: Optional[Path] = None) -> List[ForecastRun]:
        """Run the complete pipeline"""
        logger.info("Starting modular weather pipeline")

        # Setup
        if not self.grid_info:
            self.initialize_grid()

        self.setup_target_location(target_lat, target_lon, target_name)

        # Set output directory
        if output_dir is None:
            output_dir = Path.cwd() / 'data'
        else:
            output_dir = Path(output_dir)

        # Check if we should skip download and use existing files
        if self.config.skip_download:
            logger.info("Skip download mode enabled - looking for existing GRIB files")
            existing_files = find_existing_grib_files()

            if not existing_files:
                logger.error("No existing GRIB files found in data/raw/. Cannot proceed in skip-download mode.")
                logger.info("Run without --skip-download to download new data first.")
                return []

            # Create run info from existing files
            forecast_runs_info = []
            for run_time_str, variables_files in existing_files.items():
                # Check if this run has the requested variables
                has_required_vars = all(var in variables_files for var in self.config.variables)
                if has_required_vars:
                    forecast_runs_info.append({
                        'run_time': run_time_str,
                        'ensembles': [],  # Not needed when using existing files
                        'num_ensembles': 0
                    })

            logger.info(f"Found {len(forecast_runs_info)} runs with required variables in existing files")

        else:
            # Discover runs normally
            forecast_runs_info = await self.discover_forecast_runs()

        # Process each run
        completed_runs = []
        for run_info in forecast_runs_info:
            try:
                if self.config.skip_download:
                    # Use existing files
                    run_time_str = run_info['run_time']
                    existing_files_map = find_existing_grib_files()
                    files_by_variable = existing_files_map.get(run_time_str, {})

                    if not files_by_variable:
                        logger.warning(f"No files found for run {run_time_str}, skipping")
                        continue

                    forecast_run = await self.process_forecast_run_from_files(run_info, files_by_variable)
                else:
                    # Normal download and process
                    forecast_run = await self.process_forecast_run(run_info)

                if not forecast_run:
                    logger.warning(f"Failed to process run {run_info.get('run_time', 'unknown')}")
                    continue
                
                # Save outputs
                saved_files = save_all_outputs(
                    forecast_run,
                    output_dir,
                    save_ensembles=self.config.save_individual_ensembles,
                    save_statistics=self.config.save_statistics,
                    save_netcdf=self.config.save_netcdf_backup
                )
                
                logger.info(f"Saved outputs for run {forecast_run.run_time}")
                completed_runs.append(forecast_run)
                
            except Exception as e:
                logger.error(f"Error processing run {run_info.get('run_time', 'unknown')}: {e}")
                continue
        
        logger.info(f"Pipeline completed successfully. Processed {len(completed_runs)} runs.")
        return completed_runs


# Convenience function for simple usage
async def run_weather_pipeline(target_lat: float = 48.1486, 
                             target_lon: float = 17.1077,
                             target_name: str = "Bratislava",
                             num_runs: int = 4,
                             variables: List[str] = None,
                             output_dir: Optional[Path] = None,
                             **kwargs) -> List[ForecastRun]:
    """
    Run the weather pipeline with simple parameters.
    
    Args:
        target_lat: Target latitude
        target_lon: Target longitude  
        target_name: Location name
        num_runs: Number of forecast runs to process
        variables: List of variables to process
        output_dir: Output directory
        **kwargs: Additional configuration options
    
    Returns:
        List of processed forecast runs
    """
    if variables is None:
        variables = ['TOT_PREC', 'VMAX_10M']
    
    # Create configuration
    config = ProcessingConfig(
        num_runs=num_runs,
        variables=variables,
        output_dir=str(output_dir) if output_dir else None,
        **kwargs
    )
    
    # Create and run orchestrator
    orchestrator = PipelineOrchestrator(config)
    return await orchestrator.run_pipeline(target_lat, target_lon, target_name, output_dir)