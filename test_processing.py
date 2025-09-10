#!/usr/bin/env python3
"""
Test the processing of downloaded GRIB files
"""
import sys
from pathlib import Path
import logging

# Add utils to path
sys.path.append(str(Path(__file__).parent))

from utils.models import ProcessingConfig
from utils.pipeline_orchestrator import PipelineOrchestrator

# Setup logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def test_processing():
    """Test processing of existing GRIB files"""
    
    # Find existing GRIB files
    raw_dir = Path("data/raw")
    tot_prec_files = list(raw_dir.glob("*TOT_PREC*.grib2"))
    vmax_files = list(raw_dir.glob("*VMAX_10M*.grib2"))
    
    logger.info(f"Found {len(tot_prec_files)} TOT_PREC files")
    logger.info(f"Found {len(vmax_files)} VMAX_10M files")
    
    if not tot_prec_files and not vmax_files:
        logger.error("No GRIB files found")
        return False
    
    # Setup configuration
    config = ProcessingConfig(num_runs=1, variables=['TOT_PREC', 'VMAX_10M'])
    orchestrator = PipelineOrchestrator(config)
    
    # Initialize grid
    grid_info = orchestrator.initialize_grid()
    target_location = orchestrator.setup_target_location(48.1486, 17.1077, "Bratislava")
    
    # Test TOT_PREC processing
    if tot_prec_files:
        logger.info("Testing TOT_PREC processing...")
        # Take first 5 files from ensemble 01
        ensemble_01_files = [f for f in tot_prec_files if '_e01_' in str(f)][:5]
        logger.info(f"Processing {len(ensemble_01_files)} TOT_PREC files")
        
        ensembles = orchestrator.process_ensemble_data(ensemble_01_files, 'TOT_PREC')
        logger.info(f"Processed {len(ensembles)} TOT_PREC ensembles")
        
        if ensembles:
            ens = ensembles[0]
            logger.info(f"Sample ensemble: {ens.ensemble_id} with {len(ens.times)} time steps")
            logger.info(f"Sample values: {ens.values[:3]}")
    
    # Test VMAX_10M processing
    if vmax_files:
        logger.info("Testing VMAX_10M processing...")
        # Take first 5 files from ensemble 01
        ensemble_01_files = [f for f in vmax_files if '_e01_' in str(f)][:5]
        logger.info(f"Processing {len(ensemble_01_files)} VMAX_10M files")
        
        ensembles = orchestrator.process_ensemble_data(ensemble_01_files, 'VMAX_10M')
        logger.info(f"Processed {len(ensembles)} VMAX_10M ensembles")
        
        if ensembles:
            ens = ensembles[0]
            logger.info(f"Sample ensemble: {ens.ensemble_id} with {len(ens.times)} time steps")
            logger.info(f"Sample values: {ens.values[:3]}")
    
    return True

if __name__ == '__main__':
    print("Testing processing of existing GRIB files...")
    success = test_processing()
    print(f"Test result: {'PASSED' if success else 'FAILED'}")