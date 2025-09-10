"""
Orchestration utilities for ICON-D2-RUC-EPS workflow management
"""

import json
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys

import numpy as np
import pandas as pd
import xarray as xr
from tqdm import tqdm

# Add parent directory to path for imports
parent_dir = Path(__file__).parent.parent
sys.path.insert(0, str(parent_dir))

try:
    # Try importing config module
    import config
    DATA_DIR = config.DATA_DIR
    OUTPUTS_DIR = config.OUTPUTS_DIR
    PROCESSED_DATA_DIR = config.PROCESSED_DATA_DIR
    LOCATIONS = config.LOCATIONS
except ImportError:
    # Fallback: define paths manually
    BASE_DIR = Path(__file__).parent.parent
    DATA_DIR = BASE_DIR / "data"
    OUTPUTS_DIR = BASE_DIR / "outputs"
    PROCESSED_DATA_DIR = DATA_DIR / "processed"
    LOCATIONS = {
        'Berlin': {'lat': 52.52, 'lon': 13.40},
        'Munich': {'lat': 48.14, 'lon': 11.58},
        'Hamburg': {'lat': 53.55, 'lon': 10.00},
        'Frankfurt': {'lat': 50.11, 'lon': 8.68},
        'Cologne': {'lat': 50.94, 'lon': 6.96},
        'Stuttgart': {'lat': 48.78, 'lon': 9.18},
        'Dresden': {'lat': 51.05, 'lon': 13.74},
        'Hannover': {'lat': 52.37, 'lon': 9.73},
    }

from utils import discovery, download, grid, precipitation, visualization


class WorkflowState:
    """Manages workflow state and progress tracking"""

    def __init__(self, state_file: Optional[Path] = None):
        self.state_file = state_file if state_file is not None else (DATA_DIR / "workflow_state.json")
        self.state = self._load_state()

    def _load_state(self) -> Dict[str, Any]:
        """Load workflow state from file"""
        if self.state_file.exists():
            try:
                with open(self.state_file, "r") as f:
                    return json.load(f)
            except Exception as e:
                print(f"Warning: Could not load state file: {e}")

        return self._default_state()

    def _default_state(self) -> Dict[str, Any]:
        """Create default workflow state"""
        return {
            "workflow_id": f"icon_ruc_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "created": datetime.now().isoformat(),
            "last_updated": datetime.now().isoformat(),
            "steps": {
                "discovery": {
                    "status": "pending",
                    "start_time": None,
                    "end_time": None,
                    "error": None,
                },
                "download": {
                    "status": "pending",
                    "start_time": None,
                    "end_time": None,
                    "error": None,
                },
                "regrid": {
                    "status": "pending",
                    "start_time": None,
                    "end_time": None,
                    "error": None,
                },
                "process": {
                    "status": "pending",
                    "start_time": None,
                    "end_time": None,
                    "error": None,
                },
                "visualize": {
                    "status": "pending",
                    "start_time": None,
                    "end_time": None,
                    "error": None,
                },
            },
            "discovered_runs": [],
            "downloaded_files": [],
            "regridded_files": [],
            "processed_files": [],
            "config": {},
            "statistics": {},
        }

    def save_state(self):
        """Save workflow state to file"""
        self.state["last_updated"] = datetime.now().isoformat()

        try:
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.state_file, "w") as f:
                json.dump(self.state, f, indent=2, default=str)
        except Exception as e:
            print(f"Warning: Could not save state file: {e}")

    def update_step(self, step_name: str, status: str, error: Optional[str] = None):
        """Update step status"""
        if step_name not in self.state["steps"]:
            self.state["steps"][step_name] = {}

        step = self.state["steps"][step_name]
        step["status"] = status
        step["error"] = error

        if status == "running" and step.get("start_time") is None:
            step["start_time"] = datetime.now().isoformat()
        elif status in ["completed", "failed"]:
            step["end_time"] = datetime.now().isoformat()

        self.save_state()

    def get_step_status(self, step_name: str) -> str:
        """Get status of a workflow step"""
        return self.state["steps"].get(step_name, {}).get("status", "pending")

    def is_step_complete(self, step_name: str) -> bool:
        """Check if a step is completed"""
        return self.get_step_status(step_name) == "completed"

    def can_run_step(self, step_name: str) -> bool:
        """Check if a step can be run (dependencies met)"""
        dependencies = {
            "discovery": [],
            "download": ["discovery"],
            "regrid": ["download"],
            "process": ["regrid"],
            "visualize": ["process"],
        }

        for dep in dependencies.get(step_name, []):
            if not self.is_step_complete(dep):
                return False
        return True


class WorkflowOrchestrator:
    """Main orchestrator for ICON-D2-RUC-EPS data processing workflow"""

    def __init__(
        self,
        num_runs: int = 4,
        forecast_hours: Optional[float] = None,
        step_interval_minutes: int = 15,
        max_workers: int = 4,
        state_file: Optional[Path] = None,
    ):
        """
        Initialize workflow orchestrator.

        Args:
            num_runs: Number of forecast runs to process
            forecast_hours: Limit forecast range in hours (None = all)
            step_interval_minutes: Download files at these minute intervals (15 = 00,15,30,45)
            max_workers: Maximum number of parallel workers
            state_file: Path to workflow state file
        """
        self.num_runs = num_runs
        self.forecast_hours = forecast_hours
        self.step_interval_minutes = step_interval_minutes
        self.max_workers = max_workers
        self.state = WorkflowState(state_file)

        # Initialize processors
        self.precip_processor = precipitation.PrecipitationProcessor()

        # Grid setup (will be loaded/cached)
        self.icon_lats = None
        self.icon_lons = None
        self.target_grids = None

        print(f"Workflow orchestrator initialized")
        print(f"  Workflow ID: {self.state.state['workflow_id']}")
        print(f"  Target runs: {self.num_runs}")
        print(
            f"  Forecast window: {self.forecast_hours if self.forecast_hours else 'All'} hours"
        )
        print(f"  Step interval: {self.step_interval_minutes} minutes")
        print(f"  Max workers: {self.max_workers}")

    def run_full_workflow(
        self, skip_completed: bool = True, max_files_per_step: Optional[int] = None
    ) -> bool:
        """
        Run the complete workflow from discovery to visualization.

        Args:
            skip_completed: Skip steps that are already completed
            max_files_per_step: Limit files processed (for testing)

        Returns:
            bool: True if workflow completed successfully
        """
        print(f"\\nStarting full workflow...")
        print(f"  Skip completed steps: {skip_completed}")

        steps = [
            ("discovery", self.run_discovery),
            ("download", lambda: self.run_download(max_files=max_files_per_step)),
            ("regrid", lambda: self.run_regrid(max_files=max_files_per_step)),
            ("process", lambda: self.run_process()),
            ("visualize", lambda: self.run_visualization()),
        ]

        for step_name, step_func in steps:
            if skip_completed and self.state.is_step_complete(step_name):
                print(f"\\n⏭️  Skipping {step_name} (already completed)")
                continue

            if not self.state.can_run_step(step_name):
                print(f"\\n❌ Cannot run {step_name} - dependencies not met")
                return False

            print(f"\\n🚀 Running step: {step_name}")
            success = step_func()

            if not success:
                print(f"❌ Step {step_name} failed")
                return False

            print(f"✅ Step {step_name} completed")

        print(f"\\n🎉 Full workflow completed successfully!")
        self._generate_final_report()
        return True

    def run_discovery(self) -> bool:
        """Run forecast discovery step"""
        self.state.update_step("discovery", "running")

        try:
            print("Discovering available forecast runs...")

            # Get available runs
            available_runs = discovery.get_available_run_times(limit=self.num_runs)

            if not available_runs:
                raise ValueError("No forecast runs found")

            print(f"Found {len(available_runs)} recent runs")

            # Discover ensembles and steps for each run
            discovery_data = {}
            total_original_files = 0
            total_filtered_files = 0

            for run_time in available_runs:
                print(f"  Discovering run: {run_time}")

                # Get ensembles
                ensembles = discovery.get_available_ensembles(run_time)
                if not ensembles:
                    print(f"    Warning: No ensembles found for {run_time}")
                    continue

                discovery_data[run_time] = {}
                run_original = 0
                run_filtered = 0

                for ensemble in ensembles:
                    # Get all available steps
                    all_steps = discovery.get_available_steps(run_time, ensemble)
                    run_original += len(all_steps)

                    if all_steps:
                        # Apply time filtering
                        filtered_steps = (
                            discovery.filter_steps_by_time_window_and_interval(
                                all_steps,
                                forecast_hours=self.forecast_hours,
                                interval_minutes=self.step_interval_minutes,
                            )
                        )
                        run_filtered += len(filtered_steps)

                        if filtered_steps:
                            discovery_data[run_time][ensemble] = filtered_steps

                total_original_files += run_original
                total_filtered_files += run_filtered

                print(f"    Found {len(discovery_data.get(run_time, {}))} ensembles")
                if run_original > 0:
                    reduction = (1 - run_filtered / run_original) * 100
                    print(
                        f"    Steps: {run_filtered}/{run_original} (reduced {reduction:.1f}%)"
                    )

            # Report overall filtering results
            if total_original_files > 0:
                overall_reduction = (
                    1 - total_filtered_files / total_original_files
                ) * 100
                print(f"\nFiltering Summary:")
                print(f"  Original files: {total_original_files:,}")
                print(f"  Filtered files: {total_filtered_files:,}")
                print(f"  Data reduction: {overall_reduction:.1f}%")
                if self.forecast_hours:
                    print(f"  Time window: 0 - {self.forecast_hours} hours")
                print(f"  Step interval: {self.step_interval_minutes} minutes")

            # Save discovery results
            self.state.state["discovered_runs"] = available_runs
            self.state.state["discovery_data"] = discovery_data

            # Use filtered file count
            self.state.state["statistics"][
                "total_files_discovered"
            ] = total_filtered_files
            self.state.state["statistics"][
                "original_files_discovered"
            ] = total_original_files
            self.state.state["statistics"]["filtering_reduction_percent"] = (
                overall_reduction if total_original_files > 0 else 0
            )

            print(f"Total files to download: {total_filtered_files:,}")

            # Save discovery to JSON file
            discovery_file = (
                DATA_DIR / f"discovery_{self.state.state['workflow_id']}.json"
            )
            with open(discovery_file, "w") as f:
                json.dump(
                    {
                        "runs": available_runs,
                        "discovery_data": discovery_data,
                        "total_files": total_filtered_files,
                        "original_files": total_original_files,
                        "filtering": {
                            "forecast_hours": self.forecast_hours,
                            "step_interval_minutes": self.step_interval_minutes,
                            "reduction_percent": (
                                overall_reduction if total_original_files > 0 else 0
                            ),
                        },
                        "timestamp": datetime.now().isoformat(),
                    },
                    f,
                    indent=2,
                )

            print(f"Discovery results saved: {discovery_file}")

            self.state.update_step("discovery", "completed")
            return True

        except Exception as e:
            error_msg = f"Discovery failed: {str(e)}"
            print(f"❌ {error_msg}")
            print(f"Traceback: {traceback.format_exc()}")
            self.state.update_step("discovery", "failed", error_msg)
            return False

    def run_download(self, max_files: Optional[int] = None) -> bool:
        """Run data download step"""
        self.state.update_step("download", "running")

        try:
            print("Starting data download...")

            discovery_data = self.state.state.get("discovery_data", {})
            if not discovery_data:
                raise ValueError("No discovery data found - run discovery first")

            # Build download list
            download_list = []
            for run_time, run_data in discovery_data.items():
                for ensemble, steps in run_data.items():
                    for step in steps:
                        download_list.append((run_time, ensemble, step))

            # Limit files if requested
            if max_files:
                download_list = download_list[:max_files]
                print(
                    f"Limiting download to {len(download_list)} files (max_files={max_files})"
                )

            print(f"Downloading {len(download_list):,} files...")

            # Estimate download size
            size_info = download.estimate_download_size(discovery_data)
            print(f"Estimated download size: {size_info['estimated_size_mb']:.0f} MB")

            # Use optimized download methods
            print(f"  Using optimized download methods...")

            # Select optimal method based on file count
            file_count = len(download_list)
            if file_count < 10:
                download_method = "sequential"
            elif file_count < 100:
                download_method = "parallel"
            else:
                download_method = "auto"  # Let smart_batch_download decide

            print(f"  Selected method: {download_method} (for {file_count} files)")

            # Use optimized download functions
            if download_method == "sequential":
                raw_downloaded_files = download.batch_download(
                    download_list,
                    max_files=max_files,
                    progress_interval=max(
                        1, len(download_list) // 20
                    ),  # Update every 5%
                )
            else:
                raw_downloaded_files = download.smart_batch_download(
                    download_list,
                    method=download_method,
                    max_workers=self.max_workers,
                    max_files=max_files,
                )

            # Convert results to consistent format (Path objects)
            downloaded_files = []
            if raw_downloaded_files:
                for result in raw_downloaded_files:
                    if result:
                        file_path = Path(result) if isinstance(result, str) else result
                        if file_path.exists():
                            downloaded_files.append(
                                str(file_path)
                            )  # Store as string for JSON serialization

            self.state.state["downloaded_files"] = [str(f) for f in downloaded_files]
            self.state.state["statistics"]["files_downloaded"] = len(downloaded_files)

            print(f"✅ Downloaded {len(downloaded_files):,} files")

            # Verify downloads
            if downloaded_files:
                verification = download.verify_downloads(downloaded_files)
                print(
                    f"Verification: {verification['valid_files']}/{verification['total_files']} files valid"
                )

                if verification["valid_files"] == 0:
                    raise ValueError("No valid files downloaded")

            self.state.update_step("download", "completed")
            return True

        except Exception as e:
            error_msg = f"Download failed: {str(e)}"
            print(f"❌ {error_msg}")
            print(f"Traceback: {traceback.format_exc()}")
            self.state.update_step("download", "failed", error_msg)
            return False

    def run_regrid(self, max_files: Optional[int] = None) -> bool:
        """Run regridding step"""
        self.state.update_step("regrid", "running")

        try:
            print("Starting regridding...")

            # Setup grid if not already done
            if self.icon_lats is None:
                print("  Setting up ICON grid definition...")
                self.icon_lats, self.icon_lons = grid.download_icon_grid_definition()

                if self.icon_lats is None:
                    raise ValueError("Failed to load ICON grid definition")

                print(f"  ICON grid loaded: {len(self.icon_lats):,} points")

            if self.target_grids is None:
                print("  Creating target regular grid...")
                self.target_grids = grid.create_regular_grid()

                target_lats, target_lons, _, _ = self.target_grids
                print(f"  Regular grid: {len(target_lats)}×{len(target_lons)} points")

                # Validate coverage
                validation = grid.validate_grid_coverage(
                    self.icon_lats, self.icon_lons, self.target_grids
                )
                if not validation["full_coverage"]:
                    print("  ⚠️  Warning: Target grid extends beyond ICON coverage")

            # Get downloaded files
            downloaded_files = [
                Path(f) for f in self.state.state.get("downloaded_files", [])
            ]
            if not downloaded_files:
                raise ValueError("No downloaded files found - run download first")

            # Filter existing files
            existing_files = [f for f in downloaded_files if f.exists()]
            print(f"  Found {len(existing_files)} existing GRIB2 files")

            if max_files:
                existing_files = existing_files[:max_files]
                print(f"  Limiting regridding to {len(existing_files)} files")

            # Use optimized regridding methods
            print(f"  Using optimized regridding methods...")

            # Select optimal method based on file count
            file_count = len(existing_files)
            if file_count < 10:
                regrid_method = "sequential"
            elif file_count < 100:
                regrid_method = "parallel"
            else:
                regrid_method = "batch"  # Use batch processing for large datasets

            print(f"  Selected method: {regrid_method} (for {file_count} files)")

            regridded_files = []
            failed_files = []

            if regrid_method == "batch" and file_count >= 10:
                # Use optimized batch regridding for better performance
                try:
                    print(f"  Using batch regridding for {file_count} files...")
                    regridded_results = grid.smart_regrid(
                        existing_files,
                        method="auto",
                        max_memory_gb=8,  # Conservative memory limit
                        target_resolution=0.02,
                    )

                    if regridded_results:
                        regridded_files.extend(
                            [
                                str(f)
                                for f in regridded_results
                                if f and Path(f).exists()
                            ]
                        )
                        print(
                            f"  ✅ Batch regridding completed: {len(regridded_files)} files"
                        )
                    else:
                        print(
                            f"  ⚠️ Batch regridding returned no results, falling back to individual processing"
                        )
                        regrid_method = "parallel"  # Fallback

                except Exception as e:
                    print(f"  ❌ Batch regridding failed: {e}")
                    print(f"  Falling back to individual file processing...")
                    regrid_method = "parallel"  # Fallback

            # Fallback to individual file processing if batch failed or not applicable
            if regrid_method in ["sequential", "parallel"] or not regridded_files:
                print(
                    f"  Processing files individually using {self.max_workers} workers..."
                )

                def regrid_single_file(filepath):
                    """Regrid a single GRIB2 file"""
                    try:
                        output_path = PROCESSED_DATA_DIR / (
                            filepath.stem + "_regridded.nc"
                        )

                        # Skip if already exists
                        if output_path.exists():
                            return str(output_path), None

                        # Regrid the file
                        ds_regridded = grid.load_and_regrid_grib_file(
                            filepath,
                            self.icon_lats,
                            self.icon_lons,
                            self.target_grids,
                            method="linear",
                        )

                        if ds_regridded is not None:
                            # Save regridded data
                            output_path.parent.mkdir(parents=True, exist_ok=True)
                            ds_regridded.to_netcdf(output_path)
                            ds_regridded.close()

                            return str(output_path), None
                        else:
                            return None, f"Regridding failed for {filepath.name}"

                    except Exception as e:
                        return None, f"Error regridding {filepath.name}: {str(e)}"

                # Process remaining files that weren't handled by batch
                files_to_process = existing_files
                if regridded_files:
                    # Filter out already processed files
                    processed_stems = {
                        Path(f).stem.replace("_regridded", "") for f in regridded_files
                    }
                    files_to_process = [
                        f for f in existing_files if f.stem not in processed_stems
                    ]

                if files_to_process:
                    workers = 1 if regrid_method == "sequential" else self.max_workers

                    with ThreadPoolExecutor(max_workers=workers) as executor:
                        # Submit all tasks
                        future_to_file = {
                            executor.submit(regrid_single_file, filepath): filepath
                            for filepath in files_to_process
                        }

                        # Process results with progress bar
                        with tqdm(
                            total=len(files_to_process), desc="Regridding"
                        ) as pbar:
                            for future in as_completed(future_to_file):
                                filepath = future_to_file[future]

                                try:
                                    result_path, error = future.result()

                                    if result_path:
                                        regridded_files.append(result_path)
                                    else:
                                        failed_files.append((str(filepath), error))

                                except Exception as e:
                                    failed_files.append(
                                        (str(filepath), f"Exception: {str(e)}")
                                    )

                        pbar.update(1)

            print(f"✅ Regridded {len(regridded_files)} files")
            if failed_files:
                print(f"⚠️  {len(failed_files)} files failed to regrid")
                for filepath, error in failed_files[:5]:  # Show first 5 errors
                    print(f"    {Path(filepath).name}: {error}")

            self.state.state["regridded_files"] = regridded_files
            self.state.state["failed_regrids"] = failed_files
            self.state.state["statistics"]["files_regridded"] = len(regridded_files)
            self.state.state["statistics"]["regrid_failures"] = len(failed_files)

            if len(regridded_files) == 0:
                raise ValueError("No files were successfully regridded")

            self.state.update_step("regrid", "completed")
            return True

        except Exception as e:
            error_msg = f"Regridding failed: {str(e)}"
            print(f"❌ {error_msg}")
            print(f"Traceback: {traceback.format_exc()}")
            self.state.update_step("regrid", "failed", error_msg)
            return False

    def run_process(self) -> bool:
        """Run data processing step"""
        self.state.update_step("process", "running")

        try:
            print("Starting data processing...")

            # Get regridded files
            regridded_files = [
                Path(f) for f in self.state.state.get("regridded_files", [])
            ]
            if not regridded_files:
                raise ValueError("No regridded files found - run regrid first")

            existing_files = [f for f in regridded_files if f.exists()]
            print(f"  Found {len(existing_files)} regridded files")

            if not existing_files:
                raise ValueError("No regridded files exist")

            # Load and combine data
            print("  Loading regridded datasets...")
            datasets = []

            for filepath in existing_files:
                try:
                    ds = xr.open_dataset(filepath)

                    # Add metadata from filename
                    # Expected format: icon_d2_ruc_eps_TOT_PREC_YYYYMMDD_HH_eXX_PTXXXHXXM_regridded.nc
                    filename_parts = filepath.stem.replace("_regridded", "").split("_")
                    if (
                        len(filename_parts) >= 9
                    ):  
                        ds.attrs["run_date"] = filename_parts[6]  # YYYYMMDD
                        ds.attrs["run_hour"] = filename_parts[7]  # HH
                        ds.attrs["ensemble"] = filename_parts[8]   # eXX
                        if len(filename_parts) >= 10:
                            ds.attrs["step"] = filename_parts[9]   # PTXXXHXXM

                    datasets.append(ds)

                except Exception as e:
                    print(f"    Warning: Could not load {filepath.name}: {e}")

            if not datasets:
                raise ValueError("No datasets could be loaded")

            print(f"  Loaded {len(datasets)} datasets")

            # Organize data by run and ensemble
            print("  Organizing data by runs and ensembles...")

            run_groups = {}
            for ds in datasets:
                run_key = f"{ds.attrs.get('run_date', 'unknown')}_{ds.attrs.get('run_hour', 'unknown')}"
                ensemble = ds.attrs.get("ensemble", "unknown")

                if run_key not in run_groups:
                    run_groups[run_key] = {}
                if ensemble not in run_groups[run_key]:
                    run_groups[run_key][ensemble] = []

                run_groups[run_key][ensemble].append(ds)

            print(f"  Organized into {len(run_groups)} runs")

            processed_datasets = []

            for run_key, run_data in run_groups.items():
                print(f"  Processing run: {run_key}")

                # Combine time steps for each ensemble
                ensemble_datasets = []

                for ensemble, ds_list in run_data.items():
                    if len(ds_list) > 1:
                        # Sort by step time
                        ds_list.sort(key=lambda x: x.attrs.get("step", ""))

                        # Combine along time dimension
                        try:
                            # Create step coordinate from step strings
                            steps = []
                            for ds in ds_list:
                                step_str = ds.attrs.get("step", "PT000H00M")
                                # Parse step string (PTXXXHXXM)
                                if (
                                    "PT" in step_str
                                    and "H" in step_str
                                    and "M" in step_str
                                ):
                                    h_part = step_str.split("H")[0].replace("PT", "")
                                    m_part = step_str.split("H")[1].replace("M", "")
                                    hours = int(h_part) if h_part else 0
                                    minutes = int(m_part) if m_part else 0
                                    total_hours = hours + minutes / 60.0
                                    steps.append(total_hours)
                                else:
                                    steps.append(
                                        len(steps) * 0.25
                                    )  # Default 15min intervals

                            # Add step coordinate
                            for i, ds in enumerate(ds_list):
                                ds = ds.expand_dims("step")
                                ds = ds.assign_coords(step=[steps[i]])
                                ds_list[i] = ds

                            # Concatenate along step dimension
                            ensemble_ds = xr.concat(ds_list, dim="step")
                            ensemble_ds = ensemble_ds.assign_coords(
                                ensemble=int(ensemble.replace("e", ""))
                            )
                            ensemble_datasets.append(ensemble_ds)

                        except Exception as e:
                            print(
                                f"    Warning: Could not combine steps for {ensemble}: {e}"
                            )
                            # Use first dataset
                            if ds_list:
                                ds = ds_list[0].expand_dims("step")
                                ds = ds.assign_coords(step=[0.0])
                                ds = ds.assign_coords(
                                    ensemble=int(ensemble.replace("e", ""))
                                )
                                ensemble_datasets.append(ds)

                if ensemble_datasets:
                    try:
                        # Combine ensembles
                        run_ds = xr.concat(ensemble_datasets, dim="ensemble")

                        # Add run time coordinate
                        run_date = run_key.split("_")[0]
                        run_hour = run_key.split("_")[1]
                        run_time = pd.to_datetime(
                            f"{run_date} {run_hour}:00", format="%Y%m%d %H:%M"
                        )
                        run_ds = run_ds.expand_dims("run_time")
                        run_ds = run_ds.assign_coords(run_time=[run_time])

                        processed_datasets.append(run_ds)
                        print(f"    Combined {len(ensemble_datasets)} ensembles")

                    except Exception as e:
                        print(
                            f"    Warning: Could not combine ensembles for {run_key}: {e}"
                        )

            if not processed_datasets:
                raise ValueError("No datasets could be processed")

            # Combine all runs
            print("  Combining all runs...")
            combined_data = xr.concat(processed_datasets, dim="run_time")

            print(f"  Combined dataset shape: {combined_data.tp.shape}")
            print(f"  Dimensions: {dict(combined_data.dims)}")

            # Apply precipitation processing
            print("  Processing precipitation data...")

            # Deaccumulate precipitation
            deacc_data = self.precip_processor.deaccumulate(combined_data)

            # Calculate ensemble statistics
            stats_data = self.precip_processor.calculate_ensemble_statistics(deacc_data)

            # Calculate percentiles
            percentile_data = self.precip_processor.calculate_percentiles(stats_data)

            # Convert units to mm/h
            final_data = self.precip_processor.convert_units(
                percentile_data, target_unit="mm/h"
            )

            # Extract point location data
            print("  Extracting point location data...")
            viz = visualization.ForecastVisualizer(final_data)

            location_data = {}
            for location, coords in LOCATIONS.items():
                try:
                    point_data = viz.extract_point(coords["lat"], coords["lon"])
                    location_data[location] = point_data
                    print(f"    Extracted data for {location}")
                except Exception as e:
                    print(f"    Warning: Could not extract data for {location}: {e}")

            # Save processed data
            processed_file = (
                PROCESSED_DATA_DIR
                / f"processed_data_{self.state.state['workflow_id']}.nc"
            )
            processed_file.parent.mkdir(parents=True, exist_ok=True)
            final_data.to_netcdf(processed_file)

            # Save location data
            location_file = (
                PROCESSED_DATA_DIR
                / f"location_data_{self.state.state['workflow_id']}.nc"
            )
            if location_data:
                # Combine location datasets
                location_datasets = []
                for loc_name, loc_data in location_data.items():
                    loc_data = loc_data.expand_dims("location")
                    loc_data = loc_data.assign_coords(location=[loc_name])
                    location_datasets.append(loc_data)

                if location_datasets:
                    combined_locations = xr.concat(location_datasets, dim="location")
                    combined_locations.to_netcdf(location_file)

            # Update state
            self.state.state["processed_files"] = [str(processed_file)]
            self.state.state["location_files"] = (
                [str(location_file)] if location_data else []
            )

            # Generate summary
            summary = self.precip_processor.create_precipitation_summary(final_data)
            self.state.state["statistics"].update(
                {
                    "processing_summary": summary,
                    "locations_processed": list(location_data.keys()),
                    "final_data_shape": list(final_data.tp.shape),
                    "final_dimensions": list(final_data.dims.keys()),
                }
            )

            print(f"✅ Processing completed")
            print(f"  Processed data saved: {processed_file}")
            print(f"  Location data saved: {location_file}")
            self.precip_processor.print_summary(summary)

            # Close datasets
            for ds in datasets:
                ds.close()
            for ds in processed_datasets:
                ds.close()
            final_data.close()

            self.state.update_step("process", "completed")
            return True

        except Exception as e:
            error_msg = f"Processing failed: {str(e)}"
            print(f"❌ {error_msg}")
            print(f"Traceback: {traceback.format_exc()}")
            self.state.update_step("process", "failed", error_msg)
            return False

    def run_visualization(self) -> bool:
        """Run visualization step"""
        self.state.update_step("visualize", "running")

        try:
            print("Starting visualization...")

            # Load processed data
            location_files = self.state.state.get("location_files", [])
            if not location_files:
                raise ValueError("No location data files found - run process first")

            location_file = Path(location_files[0])
            if not location_file.exists():
                raise ValueError(f"Location data file does not exist: {location_file}")

            print(f"  Loading location data: {location_file.name}")
            location_data = xr.open_dataset(location_file)

            # Initialize visualizer
            viz = visualization.ForecastVisualizer(location_data)

            # Create visualizations for each location
            visualizations_created = []

            for location in location_data.location.values:
                print(f"  Creating visualizations for {location}...")

                try:
                    # Extract point data for this location
                    point_data = location_data.sel(location=location)

                    # Create ensemble comparison plot
                    output_path = (
                        OUTPUTS_DIR
                        / f"forecast_comparison_{location}_{self.state.state['workflow_id']}.png"
                    )

                    fig = viz.plot_ensemble_comparison(
                        point_data,
                        variable="mean",
                        time_agg="1h",
                        # title_suffix=f" - {location}",
                    )

                    if fig:
                        fig.savefig(output_path, dpi=150, bbox_inches="tight")
                        visualizations_created.append(str(output_path))
                        print(f"    Saved: {output_path.name}")

                        import matplotlib.pyplot as plt

                        plt.close(fig)

                except Exception as e:
                    print(
                        f"    Warning: Could not create visualization for {location}: {e}"
                    )

            # Create summary visualization
            try:
                print("  Creating summary visualization...")

                # Multi-location comparison
                summary_path = (
                    OUTPUTS_DIR
                    / f"summary_comparison_{self.state.state['workflow_id']}.png"
                )

                # Multi-location comparison not implemented yet
                # fig = viz.plot_multi_location_comparison(
                #     location_data,
                #     variable="mean",
                #     locations=list(location_data.location.values)[
                #         :4
                #     ],  # First 4 locations
                # )
                fig = None  # Placeholder

                if fig:
                    fig.savefig(summary_path, dpi=150, bbox_inches="tight")
                    visualizations_created.append(str(summary_path))
                    print(f"    Saved: {summary_path.name}")

                    import matplotlib.pyplot as plt

                    plt.close(fig)

            except Exception as e:
                print(f"    Warning: Could not create summary visualization: {e}")

            # Update state
            self.state.state["visualization_files"] = visualizations_created
            self.state.state["statistics"]["visualizations_created"] = len(
                visualizations_created
            )

            print(f"✅ Visualization completed")
            print(f"  Created {len(visualizations_created)} visualizations")

            location_data.close()

            self.state.update_step("visualize", "completed")
            return True

        except Exception as e:
            error_msg = f"Visualization failed: {str(e)}"
            print(f"❌ {error_msg}")
            print(f"Traceback: {traceback.format_exc()}")
            self.state.update_step("visualize", "failed", error_msg)
            return False

    def _generate_final_report(self):
        """Generate final workflow report"""
        print("\\n" + "=" * 60)
        print("WORKFLOW COMPLETION REPORT")
        print("=" * 60)

        # Workflow info
        state = self.state.state
        print(f"Workflow ID: {state['workflow_id']}")
        print(f"Started: {state['created']}")
        print(f"Completed: {state['last_updated']}")

        # Step summary
        print("\\nStep Status:")
        for step_name, step_info in state["steps"].items():
            status = step_info["status"]
            icon = {
                "completed": "✅",
                "failed": "❌",
                "running": "🔄",
                "pending": "⏸️",
            }.get(status, "?")
            print(f"  {step_name.capitalize():<12}: {icon} {status}")

        # Statistics
        stats = state.get("statistics", {})
        if stats:
            print("\\nStatistics:")
            for key, value in stats.items():
                if isinstance(value, (int, float)):
                    print(f"  {key.replace('_', ' ').title()}: {value:,}")
                elif isinstance(value, list):
                    print(f"  {key.replace('_', ' ').title()}: {len(value)} items")

        # Files created
        file_counts = {
            "Downloaded files": len(state.get("downloaded_files", [])),
            "Regridded files": len(state.get("regridded_files", [])),
            "Processed files": len(state.get("processed_files", [])),
            "Visualizations": len(state.get("visualization_files", [])),
        }

        print("\\nFiles Created:")
        for file_type, count in file_counts.items():
            print(f"  {file_type}: {count:,}")

        # Save report
        report_file = OUTPUTS_DIR / f"workflow_report_{state['workflow_id']}.txt"
        try:
            with open(report_file, "w") as f:
                f.write("ICON-D2-RUC-EPS Workflow Report\\n")
                f.write("=" * 40 + "\\n\\n")
                f.write(f"Workflow ID: {state['workflow_id']}\\n")
                f.write(f"Started: {state['created']}\\n")
                f.write(f"Completed: {state['last_updated']}\\n\\n")

                f.write("Step Status:\\n")
                for step_name, step_info in state["steps"].items():
                    f.write(f"  {step_name}: {step_info['status']}\\n")

                f.write("\\nStatistics:\\n")
                for key, value in stats.items():
                    f.write(f"  {key}: {value}\\n")

                f.write("\\nFiles Created:\\n")
                for file_type, count in file_counts.items():
                    f.write(f"  {file_type}: {count}\\n")

            print(f"\\nReport saved: {report_file}")

        except Exception as e:
            print(f"Warning: Could not save report: {e}")

    def get_status(self) -> Dict[str, Any]:
        """Get current workflow status"""
        return {
            "workflow_id": self.state.state["workflow_id"],
            "steps": self.state.state["steps"],
            "statistics": self.state.state.get("statistics", {}),
            "last_updated": self.state.state["last_updated"],
        }

    def cleanup_temporary_files(self, keep_processed: bool = True):
        """Clean up temporary files to save space"""
        print("Cleaning up temporary files...")

        files_cleaned = 0

        # Clean up downloaded GRIB2 files (large)
        if not keep_processed:
            for filepath in self.state.state.get("downloaded_files", []):
                try:
                    path = Path(filepath)
                    if path.exists():
                        path.unlink()
                        files_cleaned += 1
                except Exception as e:
                    print(f"Warning: Could not delete {filepath}: {e}")

        # Clean up intermediate regridded files
        for filepath in self.state.state.get("regridded_files", []):
            try:
                path = Path(filepath)
                if path.exists() and not keep_processed:
                    path.unlink()
                    files_cleaned += 1
            except Exception as e:
                print(f"Warning: Could not delete {filepath}: {e}")

        print(f"Cleaned up {files_cleaned} temporary files")
