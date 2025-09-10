"""
Comprehensive processing pipeline for ICON-D2-RUC-EPS data
"""
import numpy as np
import xarray as xr
import pandas as pd
from pathlib import Path
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
sys.path.append('..')
from config import *

# Import our utilities
from .grid import (
    download_icon_grid_definition,
    create_regular_grid,
    load_and_regrid_grib_file,
    load_grid_configuration,
    save_grid_configuration,
    print_grid_info
)
from .precipitation import PrecipitationProcessor


class IconRucProcessor:
    """
    Comprehensive processor for ICON-RUC-EPS precipitation data.
    
    Handles:
    - Grid setup and caching
    - Batch regridding of GRIB2 files
    - Ensemble processing
    - Time series aggregation
    - Export to NetCDF format
    """
    
    def __init__(self, 
                 lat_range=None, 
                 lon_range=None, 
                 resolution=None,
                 interpolation_method='linear',
                 max_workers=4):
        """
        Initialize the processor.
        
        Args:
            lat_range (tuple): (min_lat, max_lat) for target grid
            lon_range (tuple): (min_lon, max_lon) for target grid
            resolution (float): target grid resolution in degrees
            interpolation_method (str): 'linear', 'nearest', or 'cubic'
            max_workers (int): number of parallel workers for processing
        """
        self.lat_range = lat_range or DEFAULT_LAT_RANGE
        self.lon_range = lon_range or DEFAULT_LON_RANGE
        self.resolution = resolution or DEFAULT_RESOLUTION
        self.interpolation_method = interpolation_method
        self.max_workers = max_workers
        
        # Initialize grid configuration
        self.icon_lats = None
        self.icon_lons = None
        self.target_grids = None
        self.grid_ready = False
        
        # Initialize precipitation processor
        self.precip_processor = PrecipitationProcessor()
        
        print(f"Initialized IconRucProcessor:")
        print(f"  Target region: {self.lat_range}°N, {self.lon_range}°E")
        print(f"  Resolution: {self.resolution}°")
        print(f"  Interpolation: {self.interpolation_method}")
        print(f"  Max workers: {self.max_workers}")
    
    def setup_grids(self, force_download=False):
        """
        Setup ICON and target grids, with caching support.
        
        Args:
            force_download (bool): Force re-download of ICON grid definition
            
        Returns:
            bool: True if successful, False otherwise
        """
        print("Setting up grid configuration...")
        
        if not force_download:
            # Try to load cached grid configuration
            print("Attempting to load cached grid configuration...")
            self.icon_lats, self.icon_lons, self.target_grids = load_grid_configuration()
        
        if self.icon_lats is None or force_download:
            print("Creating new grid configuration...")
            
            # Download ICON grid definition
            print("Downloading ICON grid definition...")
            self.icon_lats, self.icon_lons = download_icon_grid_definition()
            
            if self.icon_lats is None:
                print("❌ Failed to download ICON grid definition")
                return False
            
            # Create target regular grid
            print("Creating target regular grid...")
            self.target_grids = create_regular_grid(
                self.lat_range, 
                self.lon_range, 
                self.resolution
            )
            
            # Save configuration for reuse
            save_grid_configuration(self.icon_lats, self.icon_lons, self.target_grids)
            print("✅ Grid configuration saved")
        
        # Print grid information
        print_grid_info(self.icon_lats, self.icon_lons, self.target_grids)
        
        self.grid_ready = True
        return True
    
    def process_single_file(self, filepath):
        """
        Process a single GRIB2 file.
        
        Args:
            filepath (Path): Path to GRIB2 file
            
        Returns:
            tuple: (filepath, xr.Dataset or None, error_message or None)
        """
        if not self.grid_ready:
            return filepath, None, "Grid configuration not ready"
        
        try:
            # Load and regrid the file
            ds = load_and_regrid_grib_file(
                filepath,
                self.icon_lats,
                self.icon_lons,
                self.target_grids,
                method=self.interpolation_method
            )
            
            if ds is None:
                return filepath, None, "Regridding failed"
            
            # Add metadata about processing
            ds.attrs.update({
                'processed_by': 'IconRucProcessor',
                'processing_time': datetime.now().isoformat(),
                'interpolation_method': self.interpolation_method,
                'source_file': str(filepath)
            })
            
            return filepath, ds, None
            
        except Exception as e:
            return filepath, None, str(e)
    
    def process_file_batch(self, file_list, progress_callback=None):
        """
        Process multiple GRIB2 files in parallel.
        
        Args:
            file_list (list): List of file paths to process
            progress_callback (callable): Optional callback for progress updates
            
        Returns:
            dict: {filepath: xr.Dataset} for successfully processed files
        """
        if not self.grid_ready:
            raise RuntimeError("Grid configuration not ready. Call setup_grids() first.")
        
        print(f"Processing {len(file_list)} files with {self.max_workers} workers...")
        
        processed_data = {}
        failed_files = {}
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all files for processing
            future_to_file = {
                executor.submit(self.process_single_file, filepath): filepath 
                for filepath in file_list
            }
            
            # Collect results as they complete
            for i, future in enumerate(as_completed(future_to_file)):
                filepath, dataset, error = future.result()
                
                if dataset is not None:
                    processed_data[filepath] = dataset
                else:
                    failed_files[filepath] = error
                
                # Progress callback
                if progress_callback:
                    progress_callback(i + 1, len(file_list), filepath, dataset is not None)
                
                # Print progress periodically
                if (i + 1) % 10 == 0 or i + 1 == len(file_list):
                    elapsed = time.time() - start_time
                    rate = (i + 1) / elapsed
                    print(f"  Processed {i + 1}/{len(file_list)} files ({rate:.1f} files/sec)")
        
        # Summary
        elapsed = time.time() - start_time
        success_count = len(processed_data)
        failure_count = len(failed_files)
        
        print(f"\nProcessing completed in {elapsed/60:.1f} minutes:")
        print(f"  ✅ Success: {success_count} files")
        if failure_count > 0:
            print(f"  ❌ Failed: {failure_count} files")
            for filepath, error in list(failed_files.items())[:5]:  # Show first 5 failures
                print(f"    {Path(filepath).name}: {error}")
            if failure_count > 5:
                print(f"    ... and {failure_count - 5} more")
        
        return processed_data
    
    def combine_ensemble_data(self, processed_data, run_time_str, ensemble_mapping):
        """
        Combine individual files into ensemble dataset.
        
        Args:
            processed_data (dict): Results from process_file_batch
            run_time_str (str): Run time identifier
            ensemble_mapping (dict): {filepath: (ensemble, step)} mapping
            
        Returns:
            xr.Dataset: Combined ensemble dataset
        """
        print(f"Combining ensemble data for run {run_time_str}...")
        
        # Group files by ensemble and step
        ensemble_data = {}
        
        for filepath, dataset in processed_data.items():
            if filepath in ensemble_mapping:
                ensemble, step = ensemble_mapping[filepath]
                
                if ensemble not in ensemble_data:
                    ensemble_data[ensemble] = {}
                
                ensemble_data[ensemble][step] = dataset
        
        if not ensemble_data:
            print("❌ No ensemble data to combine")
            return None
        
        # Create combined dataset
        ensemble_datasets = []
        
        for ensemble_id in sorted(ensemble_data.keys()):
            steps_data = ensemble_data[ensemble_id]
            
            if not steps_data:
                continue
            
            # Sort steps by time
            sorted_steps = sorted(steps_data.keys(), key=lambda x: self._parse_step_string(x))
            
            # Combine steps for this ensemble
            step_datasets = []
            step_coords = []
            
            for step in sorted_steps:
                dataset = steps_data[step]
                step_hours = self._parse_step_string(step)
                
                # Add step coordinate
                step_datasets.append(dataset.tp)
                step_coords.append(step_hours)
            
            if step_datasets:
                # Combine along step dimension
                ensemble_combined = xr.concat(step_datasets, dim='step')
                ensemble_combined = ensemble_combined.assign_coords(step=step_coords)
                
                # Add ensemble coordinate
                ensemble_combined = ensemble_combined.expand_dims('ensemble')
                ensemble_combined = ensemble_combined.assign_coords(ensemble=[ensemble_id])
                
                ensemble_datasets.append(ensemble_combined)
        
        if not ensemble_datasets:
            print("❌ No valid ensemble datasets created")
            return None
        
        # Combine all ensembles
        print(f"Combining {len(ensemble_datasets)} ensemble members...")
        combined_dataset = xr.concat(ensemble_datasets, dim='ensemble')
        
        # Create final dataset
        result_dataset = xr.Dataset(
            {'tp': combined_dataset},
            attrs={
                'title': 'ICON-D2-RUC-EPS Precipitation Data',
                'run_time': run_time_str,
                'processed_by': 'IconRucProcessor',
                'processing_time': datetime.now().isoformat(),
                'ensembles': len(ensemble_datasets),
                'time_steps': len(step_coords),
                'grid_resolution': f"{self.resolution}°",
                'interpolation_method': self.interpolation_method
            }
        )
        
        print(f"✅ Combined dataset shape: {result_dataset.tp.shape}")
        print(f"   Dimensions: {dict(result_dataset.tp.sizes)}")
        
        return result_dataset
    
    def _parse_step_string(self, step_str):
        """Parse step string like 'PT001H15M.grib2' to hours as float"""
        import re
        match = re.match(r'PT(\d{3})H(\d{2})M', step_str)
        if match:
            hours = int(match.group(1))
            minutes = int(match.group(2))
            return hours + minutes / 60.0
        return 0.0
    
    def apply_precipitation_processing(self, dataset, 
                                     deaccumulate=True,
                                     calculate_statistics=True,
                                     calculate_percentiles=True,
                                     calculate_probabilities=True,
                                     convert_units='mm/h'):
        """
        Apply comprehensive precipitation processing.
        
        Args:
            dataset (xr.Dataset): Input ensemble dataset
            deaccumulate (bool): Convert accumulated to rate data
            calculate_statistics (bool): Calculate ensemble statistics
            calculate_percentiles (bool): Calculate percentiles
            calculate_probabilities (bool): Calculate exceedance probabilities
            convert_units (str): Target units ('mm', 'mm/h', or None)
            
        Returns:
            xr.Dataset: Processed dataset with additional variables
        """
        print("Applying precipitation processing...")
        
        processed_data = dataset.copy()
        
        # Deaccumulate if requested
        if deaccumulate:
            print("  Deaccumulating precipitation data...")
            processed_data = self.precip_processor.deaccumulate(
                processed_data, time_dim='step'
            )
        
        # Calculate ensemble statistics
        if calculate_statistics:
            print("  Calculating ensemble statistics...")
            processed_data = self.precip_processor.calculate_ensemble_statistics(
                processed_data, ensemble_dim='ensemble'
            )
        
        # Calculate percentiles
        if calculate_percentiles:
            print("  Calculating ensemble percentiles...")
            processed_data = self.precip_processor.calculate_percentiles(
                processed_data, ensemble_dim='ensemble'
            )
        
        # Calculate exceedance probabilities
        if calculate_probabilities:
            print("  Calculating exceedance probabilities...")
            processed_data = self.precip_processor.probability_exceedance(
                processed_data, ensemble_dim='ensemble'
            )
        
        # Convert units
        if convert_units:
            print(f"  Converting units to {convert_units}...")
            time_interval = self._estimate_time_interval(processed_data)
            processed_data = self.precip_processor.convert_units(
                processed_data, 
                target_unit=convert_units,
                time_interval_minutes=time_interval
            )
        
        print("✅ Precipitation processing completed")
        return processed_data
    
    def _estimate_time_interval(self, dataset):
        """Estimate time interval from dataset coordinates"""
        if 'step' in dataset.coords and len(dataset.step) > 1:
            step_diff = float(dataset.step[1] - dataset.step[0])
            return int(step_diff * 60)  # Convert hours to minutes
        return 15  # Default assumption
    
    def save_processed_data(self, dataset, output_path, 
                          include_compression=True,
                          include_metadata=True):
        """
        Save processed dataset to NetCDF file.
        
        Args:
            dataset (xr.Dataset): Dataset to save
            output_path (Path): Output file path
            include_compression (bool): Apply compression
            include_metadata (bool): Include detailed metadata
            
        Returns:
            bool: True if successful
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        print(f"Saving processed data to: {output_path}")
        
        try:
            # Prepare encoding for compression
            encoding = {}
            if include_compression:
                for var_name in dataset.data_vars:
                    encoding[var_name] = {
                        'zlib': True,
                        'complevel': 6,
                        'shuffle': True
                    }
            
            # Add additional metadata
            if include_metadata:
                dataset.attrs.update({
                    'created': datetime.now().isoformat(),
                    'creator': 'ICON-RUC-EPS Processing Pipeline',
                    'format_version': '1.0',
                    'conventions': 'CF-1.8',
                    'institution': 'Weather Data Processing',
                    'source': 'ICON-D2-RUC-EPS via DWD OpenData'
                })
            
            # Save to NetCDF
            dataset.to_netcdf(
                output_path,
                encoding=encoding if include_compression else None,
                unlimited_dims=['step'] if 'step' in dataset.dims else None
            )
            
            # Get file size
            file_size = output_path.stat().st_size / (1024*1024)  # MB
            print(f"✅ Saved: {file_size:.1f} MB")
            
            return True
            
        except Exception as e:
            print(f"❌ Error saving file: {e}")
            return False
    
    def process_run_complete(self, file_list, ensemble_mapping, run_time_str,
                           output_path=None, processing_options=None):
        """
        Complete processing workflow for a single run.
        
        Args:
            file_list (list): List of GRIB2 files for this run
            ensemble_mapping (dict): Mapping of files to (ensemble, step)
            run_time_str (str): Run time identifier
            output_path (Path): Optional output file path
            processing_options (dict): Processing configuration options
            
        Returns:
            xr.Dataset or None: Processed dataset if successful
        """
        if not self.grid_ready:
            if not self.setup_grids():
                return None
        
        # Default processing options
        default_options = {
            'deaccumulate': True,
            'calculate_statistics': True,
            'calculate_percentiles': True,
            'calculate_probabilities': True,
            'convert_units': 'mm/h'
        }
        
        if processing_options:
            default_options.update(processing_options)
        
        print(f"Starting complete processing for run: {run_time_str}")
        print(f"  Files: {len(file_list)}")
        print(f"  Ensembles: {len(set(ens for ens, _ in ensemble_mapping.values()))}")
        
        # Step 1: Process individual files
        processed_files = self.process_file_batch(file_list)
        
        if not processed_files:
            print("❌ No files processed successfully")
            return None
        
        # Step 2: Combine into ensemble dataset
        combined_dataset = self.combine_ensemble_data(
            processed_files, run_time_str, ensemble_mapping
        )
        
        if combined_dataset is None:
            print("❌ Failed to combine ensemble data")
            return None
        
        # Step 3: Apply precipitation processing
        processed_dataset = self.apply_precipitation_processing(
            combined_dataset, **default_options
        )
        
        # Step 4: Save if requested
        if output_path:
            success = self.save_processed_data(processed_dataset, output_path)
            if not success:
                print("⚠️ Processing completed but save failed")
        
        print(f"✅ Complete processing finished for {run_time_str}")
        
        return processed_dataset
    
    def create_processing_summary(self, dataset):
        """Create summary of processed dataset"""
        summary = self.precip_processor.create_precipitation_summary(dataset)
        
        # Add processing-specific info
        summary.update({
            'processing_method': 'IconRucProcessor',
            'interpolation_method': self.interpolation_method,
            'target_resolution': self.resolution,
            'target_region': {
                'lat_range': self.lat_range,
                'lon_range': self.lon_range
            },
            'additional_variables': len([v for v in dataset.data_vars if v != 'tp'])
        })
        
        return summary
    
    def print_processing_summary(self, dataset):
        """Print comprehensive processing summary"""
        summary = self.create_processing_summary(dataset)
        
        print("\n" + "="*70)
        print("ICON-RUC PROCESSING SUMMARY")
        print("="*70)
        
        # Basic info
        print(f"Processing method: {summary.get('processing_method', 'unknown')}")
        print(f"Interpolation: {summary.get('interpolation_method', 'unknown')}")
        print(f"Resolution: {summary.get('target_resolution', 0):.1f}°")
        
        # Region
        region = summary.get('target_region', {})
        print(f"Region: {region.get('lat_range', (0,0))}°N, {region.get('lon_range', (0,0))}°E")
        
        # Data dimensions
        print(f"\nDimensions: {' × '.join(map(str, summary.get('shape', [])))}")
        print(f"Variables: {summary.get('total_ensembles', 0)} ensembles, {summary.get('total_timesteps', 0)} steps")
        print(f"Additional variables: {summary.get('additional_variables', 0)}")
        
        # Precipitation statistics
        print(f"\nPrecipitation Statistics:")
        print(f"  Units: {summary.get('units', 'unknown')}")
        print(f"  Range: {summary.get('global_min', 0):.4f} to {summary.get('global_max', 0):.4f}")
        print(f"  Mean: {summary.get('global_mean', 0):.4f}")
        print(f"  Non-zero: {summary.get('non_zero_fraction', 0)*100:.1f}%")
        
        if summary.get('forecast_range'):
            print(f"  Forecast range: {summary['forecast_range']}")
        
        print("="*70)