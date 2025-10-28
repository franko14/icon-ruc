#!/usr/bin/env python3
"""
Data Models for Weather Pipeline
===============================

Defines data structures and validation models for weather forecast processing.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Union, Any
from datetime import datetime
import json
from pathlib import Path


@dataclass
class GridInfo:
    """Information about the ICON grid"""
    lats: List[float]
    lons: List[float] 
    kdtree: Any  # scipy.spatial.KDTree
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TargetLocation:
    """Target location for extraction"""
    lat: float
    lon: float
    name: str = "Unknown"
    grid_index: Optional[int] = None
    distance_km: Optional[float] = None
    
    @property
    def coordinates(self) -> List[float]:
        return [self.lat, self.lon]


@dataclass
class WeatherVariable:
    """Configuration for a weather variable"""
    id: str  # e.g., 'TOT_PREC', 'VMAX_10M'
    name: str  # Human readable name
    unit: str  # e.g., 'mm/h', 'm/s'
    grib_shortName: str  # GRIB parameter name
    needs_deaccumulation: bool = False
    percentiles: List[int] = field(default_factory=lambda: [5, 10, 25, 50, 75, 90, 95])
    
    @classmethod
    def get_precipitation(cls) -> 'WeatherVariable':
        return cls(
            id='TOT_PREC',
            name='Precipitation',
            unit='mm/h',
            grib_shortName='tp',
            needs_deaccumulation=True
        )
    
    @classmethod
    def get_wind_speed(cls) -> 'WeatherVariable':
        return cls(
            id='VMAX_10M', 
            name='Wind Gust',
            unit='m/s',
            grib_shortName='max_i10fg',
            needs_deaccumulation=False
        )


@dataclass
class EnsembleMember:
    """Data for a single ensemble member"""
    ensemble_id: str  # e.g., '01', '02'
    times: List[str]  # ISO format timestamps
    values: List[float]  # Raw values
    accumulated_values: Optional[List[float]] = None  # For precipitation
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        data = {
            'ensemble_id': self.ensemble_id,
            'times': self.times,
            'values': self.values,
            'metadata': self.metadata
        }
        if self.accumulated_values is not None:
            data['accumulated_values'] = self.accumulated_values
        return data


@dataclass
class EnsembleStatistics:
    """Statistical data computed from ensemble members"""
    times: List[str]
    mean: List[float]
    median: List[float] 
    std: List[float]
    min: List[float]
    max: List[float]
    percentiles: Dict[str, List[float]]  # e.g., {'p05': [...], 'p95': [...]}
    num_ensembles: int
    
    def to_dict(self, variable_prefix: str = 'tp') -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization without variable prefix"""
        return {
            'times': self.times,
            'ensemble_statistics': {
                'mean': self.mean,
                'median': self.median, 
                'std': self.std,
                'min': self.min,
                'max': self.max,
                **{f'p{p}': values for p, values in self.percentiles.items()}
            },
            'num_ensembles': self.num_ensembles
        }


@dataclass
class VariableData:
    """Complete data for a weather variable"""
    variable: WeatherVariable
    ensembles: List[EnsembleMember] = field(default_factory=list)
    statistics: Optional[EnsembleStatistics] = None
    
    def to_statistics_dict(self) -> Dict[str, Any]:
        """Convert to statistics JSON format"""
        if not self.statistics:
            raise ValueError("Statistics not computed yet")
            
        return {
            'name': self.variable.name,
            'unit': self.variable.unit,
            'num_ensembles': len(self.ensembles),
            **self.statistics.to_dict()
        }


@dataclass
class ForecastRun:
    """Complete forecast run data"""
    run_time: str  # ISO format 
    location: TargetLocation
    variables: Dict[str, VariableData] = field(default_factory=dict)
    processed_at: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def get_run_id(self) -> str:
        """Get directory-safe run identifier"""
        return self.run_time.replace(':', '_')
    
    def get_directory_name(self) -> str:
        """Get directory name for this forecast run"""
        return f"forecast_{self.get_run_id()}"
    
    def to_master_json(self) -> Dict[str, Any]:
        """Convert to master JSON for frontend consumption"""
        variables_data = {}
        
        for var_id, var_data in self.variables.items():
            if var_data.statistics:
                variables_data[var_id] = var_data.to_statistics_dict()
        
        return {
            'run_time': self.get_run_id(),
            'location': self.location.name,
            'coordinates': self.location.coordinates,
            'grid_distance_km': self.location.distance_km or 0.75,
            'processed_at': self.processed_at or datetime.utcnow().isoformat(),
            'variables': variables_data
        }


@dataclass
class ProcessingConfig:
    """Configuration for pipeline processing"""
    num_runs: int = 4
    max_workers: int = 8
    variables: List[str] = field(default_factory=lambda: ['TOT_PREC', 'VMAX_10M'])
    percentiles: List[int] = field(default_factory=lambda: [5, 10, 25, 50, 75, 90, 95])

    # Extraction parameters
    extraction_method: str = 'single'  # 'single' or 'neighbors'
    neighbor_radius_km: float = 3.5
    weighting_scheme: str = 'center_weighted'

    # Download parameters
    skip_download: bool = False  # Skip download and reuse existing GRIB files

    # Output configuration
    output_dir: Optional[str] = None
    save_individual_ensembles: bool = True
    save_statistics: bool = True
    save_netcdf_backup: bool = False
    keep_raw_files: bool = False
    overwrite: bool = False
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'ProcessingConfig':
        """Create from dictionary"""
        return cls(**{k: v for k, v in config_dict.items() if k in cls.__dataclass_fields__})


class WeatherDataEncoder(json.JSONEncoder):
    """Custom JSON encoder for weather data structures"""
    
    def default(self, obj):
        if hasattr(obj, 'to_dict'):
            return obj.to_dict()
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, Path):
            return str(obj)
        return super().default(obj)


def save_json(data: Any, filepath: Union[str, Path], indent: int = 2) -> None:
    """Save data to JSON file with proper encoding"""
    filepath = Path(filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=indent, cls=WeatherDataEncoder)


def load_json(filepath: Union[str, Path]) -> Any:
    """Load data from JSON file"""
    with open(filepath, 'r') as f:
        return json.load(f)


# Variable registry for easy lookup
WEATHER_VARIABLES = {
    'TOT_PREC': WeatherVariable.get_precipitation(),
    'VMAX_10M': WeatherVariable.get_wind_speed()
}


def get_variable_config(variable_id: str) -> WeatherVariable:
    """Get configuration for a weather variable"""
    if variable_id not in WEATHER_VARIABLES:
        raise ValueError(f"Unknown variable: {variable_id}")
    return WEATHER_VARIABLES[variable_id]