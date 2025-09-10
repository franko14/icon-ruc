#!/usr/bin/env python3
"""
Quick test for extraction functionality
"""
import sys
from pathlib import Path
import logging

# Add utils to path
sys.path.append(str(Path(__file__).parent))

from utils.models import WeatherVariable, TargetLocation, GridInfo
from utils.extraction import extract_point_from_grib
from utils.pipeline_orchestrator import PipelineOrchestrator
from utils.models import ProcessingConfig

# Setup logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def test_single_extraction():
    """Test extraction from a single GRIB file"""
    
    # Find a TOT_PREC GRIB file
    raw_dir = Path("data/raw")
    grib_files = list(raw_dir.glob("*TOT_PREC*.grib2"))
    
    if not grib_files:
        logger.error("No TOT_PREC GRIB files found in data/raw/")
        return False
        
    test_file = str(grib_files[0])
    logger.info(f"Testing with file: {Path(test_file).name}")
    
    # Setup configuration
    config = ProcessingConfig(num_runs=1, variables=['TOT_PREC'])
    orchestrator = PipelineOrchestrator(config)
    
    # Initialize grid
    try:
        grid_info = orchestrator.initialize_grid()
        logger.info("Grid initialization successful")
    except Exception as e:
        logger.error(f"Grid initialization failed: {e}")
        return False
    
    # Setup target location (Bratislava)
    try:
        target_location = orchestrator.setup_target_location(48.1486, 17.1077, "Bratislava")
        logger.info(f"Target location setup successful: {target_location.distance_km:.2f}km")
    except Exception as e:
        logger.error(f"Target location setup failed: {e}")
        return False
    
    # Test extraction
    try:
        variable = WeatherVariable.get_precipitation()
        value = extract_point_from_grib(
            test_file, target_location, grid_info, variable,
            extraction_method='single'
        )
        
        if value is not None:
            logger.info(f"Extraction successful! Value: {value:.6f} {variable.unit}")
            return True
        else:
            logger.error("Extraction returned None")
            return False
            
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    print("Testing single file extraction...")
    success = test_single_extraction()
    print(f"Test result: {'PASSED' if success else 'FAILED'}")