#!/usr/bin/env python3
"""
Weather Data Pydantic Models
============================

Pydantic models for validating weather forecast JSON data structure.
Ensures consistency and prevents schema mismatches between pipeline output and frontend.

Based on the correct format from working files (e.g., forecast_2025-08-30T17%3A00.json)
"""

from typing import List, Dict, Any, Optional
from datetime import datetime
from pydantic import BaseModel, Field, field_validator, model_validator
import re


class EnsembleStatistics(BaseModel):
    """Ensemble statistics arrays for a weather variable"""
    tp_mean: List[float] = Field(..., description="Mean values for each time step")
    tp_p05: List[float] = Field(..., description="5th percentile values")
    tp_p10: List[float] = Field(..., description="10th percentile values") 
    tp_p25: List[float] = Field(..., description="25th percentile values")
    tp_p50: List[float] = Field(..., description="50th percentile (median) values")
    tp_p75: List[float] = Field(..., description="75th percentile values")
    tp_p90: List[float] = Field(..., description="90th percentile values")
    tp_p95: List[float] = Field(..., description="95th percentile values")
    
    @model_validator(mode='after')
    def validate_consistent_lengths(self):
        """Ensure all statistical arrays have the same length"""
        arrays = [getattr(self, field) for field in ['tp_mean', 'tp_p05', 'tp_p10', 'tp_p25', 'tp_p50', 'tp_p75', 'tp_p90', 'tp_p95']]
        if not arrays:
            return self
        
        first_length = len(arrays[0])
        field_names = ['tp_mean', 'tp_p05', 'tp_p10', 'tp_p25', 'tp_p50', 'tp_p75', 'tp_p90', 'tp_p95']
        for i, arr in enumerate(arrays):
            if len(arr) != first_length:
                raise ValueError(f"All ensemble statistics arrays must have the same length. "
                               f"{field_names[0]} has {first_length} elements, {field_names[i]} has {len(arr)}")
        
        return self
    
    @field_validator('tp_mean', 'tp_p05', 'tp_p10', 'tp_p25', 'tp_p50', 'tp_p75', 'tp_p90', 'tp_p95')
    @classmethod
    def validate_non_empty(cls, v):
        """Ensure arrays are not empty"""
        if not v:
            raise ValueError("Ensemble statistics arrays cannot be empty")
        return v


class WeatherVariable(BaseModel):
    """A weather variable (e.g., TOT_PREC, VMAX_10M) with its data"""
    name: Optional[str] = Field(None, description="Human-readable variable name")
    unit: Optional[str] = Field(None, description="Unit of measurement")
    num_ensembles: Optional[int] = Field(None, gt=0, description="Number of ensemble members")
    times: Optional[List[str]] = Field(None, description="ISO format timestamps for each forecast time")
    ensemble_statistics: Optional[EnsembleStatistics] = Field(None, description="Statistical arrays")
    
    @field_validator('times')
    @classmethod
    def validate_times_format(cls, v):
        """Validate that times are in ISO format"""
        if v is None:
            return v
        iso_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?$')
        for time_str in v:
            if not iso_pattern.match(time_str):
                raise ValueError(f"Time '{time_str}' is not in valid ISO format (YYYY-MM-DDTHH:MM:SS)")
        return v
    
    @model_validator(mode='after')
    def validate_times_statistics_consistency(self):
        """Ensure times and ensemble statistics have consistent lengths"""
        times = self.times
        ensemble_stats = self.ensemble_statistics
        
        if times and ensemble_stats and hasattr(ensemble_stats, 'tp_mean'):
            if len(times) != len(ensemble_stats.tp_mean):
                raise ValueError(f"Number of times ({len(times)}) must match number of "
                               f"ensemble statistics ({len(ensemble_stats.tp_mean)})")
        
        return self
    
    def is_valid(self) -> bool:
        """Check if this variable has complete data"""
        return all([
            self.name is not None,
            self.unit is not None,
            self.num_ensembles is not None,
            self.times is not None,
            self.ensemble_statistics is not None
        ])


class WeatherForecast(BaseModel):
    """Complete weather forecast data structure"""
    run_time: str = Field(..., description="Forecast run time (URL-encoded format)")
    location: str = Field(..., description="Location name")
    coordinates: List[float] = Field(..., min_items=2, max_items=2, description="[latitude, longitude]")
    grid_distance_km: float = Field(..., ge=0, description="Distance from target to nearest grid point in km")
    processed_at: str = Field(..., description="Processing timestamp in ISO format")
    variables: Dict[str, WeatherVariable] = Field(..., description="Weather variables data")
    
    @field_validator('run_time')
    @classmethod
    def validate_run_time_format(cls, v):
        """Validate run_time format (should be like '2025-08-30T17%3A00')"""
        # Allow both regular and URL-encoded formats
        pattern = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}(%3A|:)\d{2}$')
        if not pattern.match(v):
            raise ValueError(f"run_time '{v}' should be in format 'YYYY-MM-DDTHH%3AMM' or 'YYYY-MM-DDTHH:MM'")
        return v
    
    @field_validator('coordinates')
    @classmethod
    def validate_coordinates_range(cls, v):
        """Validate coordinates are in reasonable ranges"""
        lat, lon = v
        if not -90 <= lat <= 90:
            raise ValueError(f"Latitude {lat} must be between -90 and 90")
        if not -180 <= lon <= 180:
            raise ValueError(f"Longitude {lon} must be between -180 and 180")
        return v
    
    @field_validator('processed_at')
    @classmethod
    def validate_processed_at_format(cls, v):
        """Validate processed_at timestamp format"""
        try:
            datetime.fromisoformat(v.replace('Z', '+00:00'))
        except ValueError:
            raise ValueError(f"processed_at '{v}' must be a valid ISO format timestamp")
        return v
    
    @field_validator('variables')
    @classmethod
    def validate_has_precipitation(cls, v):
        """Ensure TOT_PREC variable is present"""
        if 'TOT_PREC' not in v:
            raise ValueError("variables must contain 'TOT_PREC'")
        return v


class WeatherForecastValidator:
    """Utility class for validating and fixing weather forecast data"""
    
    @staticmethod
    def validate_json_data(data: dict) -> WeatherForecast:
        """
        Validate JSON data against the weather forecast schema
        
        Args:
            data: Dictionary loaded from JSON
            
        Returns:
            Validated WeatherForecast model
            
        Raises:
            ValidationError: If data doesn't match expected schema
        """
        return WeatherForecast(**data)
    
    @staticmethod
    def validate_json_file(file_path: str) -> WeatherForecast:
        """
        Validate a JSON file against the weather forecast schema
        
        Args:
            file_path: Path to JSON file
            
        Returns:
            Validated WeatherForecast model
            
        Raises:
            ValidationError: If file doesn't match expected schema
            FileNotFoundError: If file doesn't exist
        """
        import json
        from pathlib import Path
        
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        with open(path, 'r') as f:
            data = json.load(f)
        
        return WeatherForecastValidator.validate_json_data(data)
    
    @staticmethod
    def get_schema_errors(data: dict) -> List[str]:
        """
        Get list of schema validation errors without raising exception
        
        Args:
            data: Dictionary to validate
            
        Returns:
            List of error messages (empty if valid)
        """
        try:
            WeatherForecastValidator.validate_json_data(data)
            return []
        except Exception as e:
            return [str(e)]
    
    @staticmethod
    def is_old_format(data: dict) -> bool:
        """
        Check if data uses the old incorrect format
        
        Returns:
            True if data has old format (metadata + time_series structure)
        """
        # Old format has 'metadata' and 'time_series' at root level
        # New format has direct properties like 'run_time', 'location', 'variables'
        old_keys = {'metadata', 'time_series'}
        new_keys = {'run_time', 'location', 'variables'}
        
        has_old_keys = any(key in data for key in old_keys)
        has_new_keys = any(key in data for key in new_keys)
        
        return has_old_keys and not has_new_keys
    
    @staticmethod
    def detect_format_issues(file_path: str) -> Dict[str, Any]:
        """
        Analyze a JSON file for format issues
        
        Returns:
            Dictionary with analysis results
        """
        import json
        from pathlib import Path
        
        result = {
            'file': file_path,
            'exists': False,
            'valid': False,
            'is_old_format': False,
            'errors': [],
            'size_kb': 0
        }
        
        try:
            path = Path(file_path)
            if not path.exists():
                result['errors'].append(f"File not found: {file_path}")
                return result
                
            result['exists'] = True
            result['size_kb'] = path.stat().st_size / 1024
            
            with open(path, 'r') as f:
                data = json.load(f)
            
            result['is_old_format'] = WeatherForecastValidator.is_old_format(data)
            result['errors'] = WeatherForecastValidator.get_schema_errors(data)
            result['valid'] = len(result['errors']) == 0
            
        except json.JSONDecodeError as e:
            result['errors'].append(f"Invalid JSON: {e}")
        except Exception as e:
            result['errors'].append(f"Error analyzing file: {e}")
        
        return result


# Example usage and testing
if __name__ == "__main__":
    import json
    
    # Example of correct format
    correct_example = {
        "run_time": "2025-08-30T17%3A00",
        "location": "Bratislava",
        "coordinates": [48.185872101456816, 17.1850614008809],
        "grid_distance_km": 1.4209073189200603,
        "processed_at": "2025-08-30T19:16:05.263553+00:00",
        "variables": {
            "TOT_PREC": {
                "name": "Precipitation",
                "unit": "mm/h",
                "num_ensembles": 20,
                "times": [
                    "2025-08-30T17:05:00",
                    "2025-08-30T17:10:00"
                ],
                "ensemble_statistics": {
                    "tp_mean": [0.037652589, 0.027496338],
                    "tp_p05": [0.009729004, 0.004748535],
                    "tp_p10": [0.018798828, 0.001879883],
                    "tp_p25": [0.038085938, 0.003808594],
                    "tp_p50": [0.039062500, 0.003906250],
                    "tp_p75": [0.038574219, 0.003857422],
                    "tp_p90": [0.074707031, 0.007470703],
                    "tp_p95": [0.076171875, 0.007617188]
                }
            }
        }
    }
    
    try:
        validated = WeatherForecastValidator.validate_json_data(correct_example)
        print("✅ Example data validates successfully!")
        print(f"   Location: {validated.location}")
        print(f"   Coordinates: {validated.coordinates}")
        print(f"   Variables: {list(validated.variables.keys())}")
    except Exception as e:
        print(f"❌ Validation failed: {e}")