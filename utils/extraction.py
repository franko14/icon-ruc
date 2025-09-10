#!/usr/bin/env python3
"""
Data Extraction Utilities
=========================

Functions for extracting weather data from GRIB files at specific locations.
"""

import numpy as np
import xarray as xr
from typing import List, Tuple, Optional, Dict, Any
from scipy.spatial import KDTree
import logging

from .models import GridInfo, TargetLocation, WeatherVariable

logger = logging.getLogger(__name__)


def find_nearest_grid_point(target_lat: float, target_lon: float, 
                           grid_lats: np.ndarray, grid_lons: np.ndarray, 
                           kdtree: Optional[KDTree] = None) -> Tuple[int, float]:
    """
    Find the nearest grid point to target coordinates using KDTree for efficiency.
    
    Args:
        target_lat: Target latitude
        target_lon: Target longitude  
        grid_lats: Array of grid latitudes
        grid_lons: Array of grid longitudes
        kdtree: Optional pre-built KDTree
    
    Returns:
        Tuple of (grid_index, distance_km)
    """
    # Build KDTree if not provided
    if kdtree is None:
        # Convert to radians for distance calculation
        grid_coords_rad = np.column_stack([
            np.radians(grid_lats.flatten()),
            np.radians(grid_lons.flatten())
        ])
        kdtree = KDTree(grid_coords_rad)
    
    # Find nearest point
    target_rad = np.radians([target_lat, target_lon])
    distance_rad, grid_index = kdtree.query(target_rad)
    
    # Convert distance from radians to kilometers (approximate)
    # Earth's radius ≈ 6371 km
    distance_km = distance_rad * 6371.0
    
    return grid_index, distance_km


def find_grid_neighbors(target_lat: float, target_lon: float,
                       grid_lats: np.ndarray, grid_lons: np.ndarray,
                       kdtree: Optional[KDTree] = None,
                       radius_km: float = 3.5,
                       max_neighbors: int = 20) -> Tuple[List[int], List[float]]:
    """
    Find grid points within a radius of target coordinates.
    
    Args:
        target_lat: Target latitude
        target_lon: Target longitude
        grid_lats: Array of grid latitudes
        grid_lons: Array of grid longitudes
        kdtree: Optional pre-built KDTree
        radius_km: Search radius in kilometers
        max_neighbors: Maximum number of neighbors to return
    
    Returns:
        Tuple of (neighbor_indices, distances_km)
    """
    # Build KDTree if not provided
    if kdtree is None:
        grid_coords_rad = np.column_stack([
            np.radians(grid_lats.flatten()),
            np.radians(grid_lons.flatten())
        ])
        kdtree = KDTree(grid_coords_rad)
    
    # Convert radius to radians
    radius_rad = radius_km / 6371.0
    
    # Find neighbors within radius
    target_rad = np.radians([target_lat, target_lon])
    neighbor_indices = kdtree.query_ball_point(target_rad, radius_rad)
    
    if len(neighbor_indices) == 0:
        logger.warning(f"No neighbors found within {radius_km}km")
        # Fall back to single nearest point
        return find_nearest_grid_point(target_lat, target_lon, grid_lats, grid_lons, kdtree)
    
    # Calculate distances and sort by distance
    distances_rad, _ = kdtree.query(target_rad, k=min(len(neighbor_indices), max_neighbors))
    distances_km = distances_rad * 6371.0
    
    # Limit to max_neighbors closest points
    if len(neighbor_indices) > max_neighbors:
        # Get exact distances for all neighbors and sort
        neighbor_coords = np.array([
            [np.radians(grid_lats.flatten()[i]), np.radians(grid_lons.flatten()[i])]
            for i in neighbor_indices
        ])
        all_distances = np.linalg.norm(neighbor_coords - target_rad, axis=1) * 6371.0
        sorted_indices = np.argsort(all_distances)[:max_neighbors]
        neighbor_indices = [neighbor_indices[i] for i in sorted_indices]
        distances_km = all_distances[sorted_indices]
    
    return neighbor_indices, distances_km


def calculate_weights(distances_km: List[float], 
                     weighting_scheme: str = 'center_weighted',
                     center_weight_factor: float = 4.0) -> List[float]:
    """
    Calculate weights for spatial interpolation.
    
    Args:
        distances_km: List of distances in kilometers
        weighting_scheme: Weighting scheme ('inverse_distance', 'center_weighted', 'gaussian')
        center_weight_factor: Extra weight factor for center point (closest)
    
    Returns:
        List of normalized weights
    """
    distances = np.array(distances_km)
    
    if weighting_scheme == 'inverse_distance':
        # Inverse distance weighting with small epsilon to avoid division by zero
        weights = 1.0 / (distances + 0.001)
        
    elif weighting_scheme == 'center_weighted':
        # Inverse distance with extra weight for center point
        weights = 1.0 / (distances + 0.001)
        # Give extra weight to the closest point (assumed to be first)
        if len(weights) > 0:
            closest_idx = np.argmin(distances)
            weights[closest_idx] *= center_weight_factor
            
    elif weighting_scheme == 'gaussian':
        # Gaussian weighting with sigma = 1km
        sigma_km = 1.0
        weights = np.exp(-(distances**2) / (2 * sigma_km**2))
        
    else:
        # Equal weights (simple average)
        weights = np.ones(len(distances))
    
    # Normalize weights to sum to 1
    weights = weights / np.sum(weights)
    
    return weights.tolist()


def extract_weighted_average_from_data(data_values: List[float], 
                                     grid_indices: List[int],
                                     weights: List[float]) -> float:
    """
    Extract weighted average from data values at specified grid points.
    
    Args:
        data_values: All data values from GRIB file
        grid_indices: Indices of grid points to use
        weights: Weights for each grid point
    
    Returns:
        Weighted average value
    """
    if len(grid_indices) != len(weights):
        raise ValueError("Number of indices and weights must match")
    
    # Extract values at specified indices
    selected_values = [data_values[idx] for idx in grid_indices]
    
    # Calculate weighted average
    weighted_sum = sum(val * weight for val, weight in zip(selected_values, weights))
    
    return weighted_sum


def extract_point_from_grib(grib_file: str, 
                          target_location: TargetLocation,
                          grid_info: GridInfo,
                          variable: WeatherVariable,
                          extraction_method: str = 'single',
                          neighbor_radius_km: float = 3.5,
                          weighting_scheme: str = 'center_weighted') -> Optional[float]:
    """
    Extract weather data from GRIB file at target location.
    
    Args:
        grib_file: Path to GRIB file
        target_location: Target location for extraction
        grid_info: ICON grid information
        variable: Weather variable configuration
        extraction_method: 'single' or 'neighbors'
        neighbor_radius_km: Radius for neighbor extraction
        weighting_scheme: Weighting scheme for neighbors
    
    Returns:
        Extracted value or None if extraction fails
    """
    logger.debug(f"Extracting from GRIB file: {grib_file}")
    logger.debug(f"Looking for variable: {variable.grib_shortName} ({variable.name})")
    logger.debug(f"Target location: {target_location.lat:.4f}°N, {target_location.lon:.4f}°E")
    
    try:
        # Check if file exists
        from pathlib import Path
        if not Path(grib_file).exists():
            logger.error(f"GRIB file does not exist: {grib_file}")
            return None
            
        # Open GRIB file with xarray and cfgrib
        logger.debug(f"Opening GRIB file with cfgrib...")
        ds = xr.open_dataset(grib_file, engine='cfgrib')
        logger.debug(f"Successfully opened GRIB file")
        
        # Log all available variables for debugging
        available_vars = list(ds.data_vars.keys())
        logger.debug(f"Available variables in GRIB file: {available_vars}")
        
        # Check for variable by multiple names
        var_found = None
        var_name_to_use = None
        
        # Try exact match first
        if variable.grib_shortName in ds.data_vars:
            var_found = ds[variable.grib_shortName]
            var_name_to_use = variable.grib_shortName
        else:
            # Try alternative names for common variables
            alt_names = []
            if variable.grib_shortName == 'tp':
                alt_names = ['tp', 'TOT_PREC', 'precipitation', 'prec', 'rain']
            elif variable.grib_shortName == '10si':
                alt_names = ['10si', 'VMAX_10M', 'wind_speed', 'windspeed', 'ws']
            
            for alt_name in alt_names:
                if alt_name in ds.data_vars:
                    var_found = ds[alt_name]
                    var_name_to_use = alt_name
                    logger.info(f"Found variable using alternative name: {alt_name} instead of {variable.grib_shortName}")
                    break
        
        if var_found is None:
            logger.error(f"Variable {variable.grib_shortName} not found in {grib_file}")
            logger.error(f"Available variables: {available_vars}")
            ds.close()
            return None
        
        logger.debug(f"Using variable: {var_name_to_use}")
        logger.debug(f"Variable shape: {var_found.shape}")
        logger.debug(f"Variable dimensions: {var_found.dims}")
        
        data_array = var_found
        data_values = data_array.values.flatten()
        
        logger.debug(f"Extracted {len(data_values)} data points from GRIB")
        logger.debug(f"Data value range: {np.min(data_values):.3f} to {np.max(data_values):.3f}")
        
        # Extract based on method
        if extraction_method == 'single':
            # Single nearest point
            if target_location.grid_index is not None:
                grid_index = target_location.grid_index
                logger.debug(f"Using pre-computed grid index: {grid_index}")
            else:
                logger.debug(f"Computing nearest grid point...")
                grid_index, distance = find_nearest_grid_point(
                    target_location.lat, target_location.lon,
                    grid_info.lats, grid_info.lons, grid_info.kdtree
                )
                target_location.grid_index = grid_index
                target_location.distance_km = distance
                logger.debug(f"Nearest grid index: {grid_index}, distance: {distance:.2f}km")
            
            if grid_index >= len(data_values):
                logger.error(f"Grid index {grid_index} out of bounds (data has {len(data_values)} points)")
                ds.close()
                return None
                
            extracted_value = float(data_values[grid_index])
            logger.debug(f"Extracted value at index {grid_index}: {extracted_value}")
            
        elif extraction_method == 'neighbors':
            # Multiple neighbors with weighting
            logger.debug(f"Finding neighbors within {neighbor_radius_km}km...")
            neighbor_indices, distances = find_grid_neighbors(
                target_location.lat, target_location.lon,
                grid_info.lats, grid_info.lons, grid_info.kdtree,
                radius_km=neighbor_radius_km
            )
            
            logger.debug(f"Found {len(neighbor_indices)} neighbors")
            logger.debug(f"Neighbor distances: {[f'{d:.2f}km' for d in distances[:5]]}")
            
            weights = calculate_weights(distances, weighting_scheme)
            logger.debug(f"Calculated weights: {[f'{w:.3f}' for w in weights[:5]]}")
            
            extracted_value = extract_weighted_average_from_data(
                data_values, neighbor_indices, weights
            )
            logger.debug(f"Weighted average value: {extracted_value}")
            
            # Update target location with center point info
            if target_location.grid_index is None:
                target_location.grid_index = neighbor_indices[0]
                target_location.distance_km = distances[0]
        
        else:
            raise ValueError(f"Unknown extraction method: {extraction_method}")
        
        ds.close()
        
        logger.info(f"Successfully extracted value: {extracted_value:.6f} from {Path(grib_file).name}")
        return extracted_value
        
    except Exception as e:
        logger.error(f"Error extracting from {grib_file}: {e}")
        logger.error(f"Exception type: {type(e).__name__}")
        import traceback
        logger.debug(f"Full traceback: {traceback.format_exc()}")
        return None


def extract_timeseries_from_grib_files(grib_files: List[str],
                                     target_location: TargetLocation,
                                     grid_info: GridInfo, 
                                     variable: WeatherVariable,
                                     **extraction_kwargs) -> Tuple[List[str], List[float]]:
    """
    Extract time series from multiple GRIB files.
    
    Args:
        grib_files: List of GRIB file paths
        target_location: Target location for extraction
        grid_info: ICON grid information
        variable: Weather variable configuration
        **extraction_kwargs: Additional extraction parameters
    
    Returns:
        Tuple of (times, values) where times are ISO format strings
    """
    times = []
    values = []
    
    for grib_file in grib_files:
        try:
            # Extract time from filename or GRIB metadata
            # Assuming filename format: ...step_HHHMM.grib2
            import re
            step_match = re.search(r'step_(\d{5})\.grib2', grib_file)
            if step_match:
                step_str = step_match.group(1)
                # Convert to hours for time calculation
                hours = int(step_str[:3])
                minutes = int(step_str[3:])
                total_minutes = hours * 60 + minutes
            else:
                logger.warning(f"Could not parse step from filename: {grib_file}")
                continue
            
            # Extract value
            value = extract_point_from_grib(
                grib_file, target_location, grid_info, variable, **extraction_kwargs
            )
            
            if value is not None:
                times.append(total_minutes)  # Store as minutes for now
                values.append(value)
                
        except Exception as e:
            logger.error(f"Error processing {grib_file}: {e}")
            continue
    
    return times, values


def validate_extraction_setup(target_location: TargetLocation,
                            grid_info: GridInfo,
                            variable: WeatherVariable) -> bool:
    """
    Validate that extraction setup is correct.
    
    Args:
        target_location: Target location
        grid_info: Grid information
        variable: Weather variable
    
    Returns:
        True if setup is valid
    """
    # Check if target is within grid bounds
    lat_min, lat_max = np.min(grid_info.lats), np.max(grid_info.lats)
    lon_min, lon_max = np.min(grid_info.lons), np.max(grid_info.lons)
    
    if not (lat_min <= target_location.lat <= lat_max):
        logger.error(f"Target latitude {target_location.lat} outside grid bounds [{lat_min}, {lat_max}]")
        return False
        
    if not (lon_min <= target_location.lon <= lon_max):
        logger.error(f"Target longitude {target_location.lon} outside grid bounds [{lon_min}, {lon_max}]")
        return False
    
    # Check grid info completeness
    if grid_info.kdtree is None:
        logger.warning("KDTree not built - extraction will be slower")
    
    return True