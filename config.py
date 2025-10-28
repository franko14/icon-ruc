"""
Configuration file for ICON-RUC-EPS processing
==============================================
"""
from pathlib import Path

# Multi-variable configuration for DWD ICON-D2-RUC-EPS data
VARIABLES_CONFIG = {
    'TOT_PREC': {
        'name': 'Total Precipitation',
        'units': 'mm/h',
        'processing_type': 'accumulated',
        'description': 'Accumulated precipitation',
        'base_url': 'https://opendata.dwd.de/weather/nwp/v1/m/icon-d2-ruc-eps/p/TOT_PREC/',
        'thresholds': [0.1, 0.5, 1.0, 2.0, 5.0, 10.0],
        'aggregations': ['15min', '1h', '3h', '6h', '12h', '24h'],
        'percentiles': [5, 10, 25, 50, 75, 90, 95],
        'color_scheme': ['#E6F3FF', '#CCE6FF', '#99D6FF', '#66C2FF', '#3399FF', '#0066CC'],
        'time_interval_minutes': 5,  # Data available every 5 minutes
        'is_active': True
    },
    'VMAX_10M': {
        'name': 'Maximum Wind Speed at 10m',
        'units': 'm/s',
        'processing_type': 'instantaneous',
        'description': 'Maximum wind speed at 10 meters height',
        'base_url': 'https://opendata.dwd.de/weather/nwp/v1/m/icon-d2-ruc-eps/p/VMAX_10M/',
        'thresholds': [5.0, 10.0, 15.0, 20.0, 25.0, 30.0],
        'aggregations': ['15min', '1h', '3h', '6h'],
        'percentiles': [5, 10, 25, 50, 75, 90, 95],
        'color_scheme': ['#FFF5E6', '#FFE6CC', '#FFD699', '#FFC266', '#FFAD33', '#FF8000'],
        'time_interval_minutes': 60,  # Data available only every 1 hour
        'is_active': True
    }
}

# Default variables for backward compatibility
DEFAULT_VARIABLES = ['TOT_PREC']

# Helper functions for variable URLs
def get_variable_urls(variable_id):
    """Generate URLs for a specific variable"""
    base_url = VARIABLES_CONFIG[variable_id]['base_url']
    return {
        'data_url': f"{base_url}r/",
        'ensemble_url': f"{base_url}r/{{run_time}}/e/",
        'step_url': f"{base_url}r/{{run_time}}/e/{{ensemble}}/s/",
        'download_url': f"{base_url}r/{{run_time}}/e/{{ensemble}}/s/{{step}}"
    }

# Legacy URLs (for backward compatibility)
BASE_DATA_URL = "https://opendata.dwd.de/weather/nwp/v1/m/icon-d2-ruc-eps/p/TOT_PREC/r/"
BASE_ENSEMBLE_URL = "https://opendata.dwd.de/weather/nwp/v1/m/icon-d2-ruc-eps/p/TOT_PREC/r/{run_time}/e/"
BASE_STEP_URL = "https://opendata.dwd.de/weather/nwp/v1/m/icon-d2-ruc-eps/p/TOT_PREC/r/{run_time}/e/{ensemble}/s/"
BASE_DOWNLOAD_URL = "https://opendata.dwd.de/weather/nwp/v1/m/icon-d2-ruc-eps/p/TOT_PREC/r/{run_time}/e/{ensemble}/s/{step}"

# Grid definition
GRID_URL = "https://opendata.dwd.de/weather/lib/cdo/icon_grid_0047_R19B07_L.nc.bz2"
GRID_FILE = "icon_grid_0047_R19B07_L.nc"

# Target grid settings (Germany region)
DEFAULT_LAT_RANGE = (47, 55)
DEFAULT_LON_RANGE = (5, 16)
DEFAULT_RESOLUTION = 0.02

# Directory paths (relative to notebook directory)
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
OUTPUTS_DIR = BASE_DIR / "outputs"
UTILS_DIR = BASE_DIR / "utils"

# Data subdirectories
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
GRID_DATA_DIR = DATA_DIR / "grid"

# Default processing parameters
DEFAULT_RUNS_LIMIT = 4
DEFAULT_INTERPOLATION_METHOD = 'linear'

# Forecast time window and interval settings
DEFAULT_FORECAST_HOURS = None  # None means download all available hours
FORECAST_STEP_INTERVAL = 15  # Download files at 15-minute intervals (00, 15, 30, 45)
AVAILABLE_STEP_MINUTES = 5  # ICON-RUC-EPS provides 5-minute steps
MAX_FORECAST_HOURS = 42.25  # Maximum forecast range (~169 steps × 5min = ~14 hours, but files go to ~42h)

# Predefined locations
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

# Legacy precipitation thresholds for backward compatibility (mm/h)
PRECIP_THRESHOLDS = [0.1, 0.5, 1.0, 2.0, 5.0, 10.0]

# Wind speed thresholds for probability calculations (m/s)
WIND_THRESHOLDS = [5.0, 10.0, 15.0, 20.0, 25.0, 30.0]

# Time aggregation options
TIME_AGGREGATIONS = {
    '15min': '15T',
    '1h': '1H', 
    '3h': '3H',
    '6h': '6H',
    '12h': '12H',
    '24h': '24H'
}

# Ensemble percentiles to calculate
PERCENTILES = [5, 10, 25, 50, 75, 90, 95]

# File naming patterns (multi-variable support)
GRIB_FILENAME_PATTERNS = {
    'TOT_PREC': "icon_d2_ruc_eps_TOT_PREC_{run_date}_{run_hour}_e{ensemble}_{step}",
    'VMAX_10M': "icon_d2_ruc_eps_VMAX_10M_{run_date}_{run_hour}_e{ensemble}_{step}"
}

# Legacy pattern for backward compatibility
GRIB_FILENAME_PATTERN = "icon_d2_ruc_eps_TOT_PREC_{run_date}_{run_hour}_e{ensemble}_{step}"
PROCESSED_FILENAME_PATTERN = "icon_ruc_ensemble_{date}.nc"

# Multi-variable filename patterns
def get_grib_filename_pattern(variable_id):
    """Get GRIB filename pattern for a variable"""
    return f"icon_d2_ruc_eps_{variable_id}_{{run_date}}_{{run_hour}}_e{{ensemble}}_{{step}}"

def get_processed_filename(location, variable_id, date_str):
    """Get processed filename for a location and variable"""
    if variable_id == 'TOT_PREC':
        return f"{location}_precipitation_{date_str}.nc"
    elif variable_id == 'VMAX_10M':
        return f"{location}_windspeed_{date_str}.nc"
    else:
        return f"{location}_{variable_id.lower()}_{date_str}.nc"

DISCOVERY_CACHE_FILE = DATA_DIR / "discovery_cache.json"
GRID_CACHE_FILE = GRID_DATA_DIR / GRID_FILE

# Create directories if they don't exist
def create_directories():
    """Create all necessary directories"""
    for dir_path in [DATA_DIR, OUTPUTS_DIR, RAW_DATA_DIR, PROCESSED_DATA_DIR, GRID_DATA_DIR]:
        dir_path.mkdir(parents=True, exist_ok=True)

# Logging configuration
LOGGING_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'filename': DATA_DIR / 'icon_ruc.log'
}