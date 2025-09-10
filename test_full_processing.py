#!/usr/bin/env python3
"""
Test full processing of downloaded files to create proper output structure
"""
import sys
from pathlib import Path
import logging
import asyncio

# Add utils to path
sys.path.append(str(Path(__file__).parent))

from utils.models import ProcessingConfig
from utils.pipeline_orchestrator import PipelineOrchestrator

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_full_processing():
    """Test full processing with downloaded files"""
    
    # Find existing GRIB files for 2025-08-31T06:00 run
    raw_dir = Path("data/raw")
    tot_prec_files = list(raw_dir.glob("*TOT_PREC*2025-08-31T0600*"))
    vmax_files = list(raw_dir.glob("*VMAX_10M*2025-08-31T0600*"))
    
    # Take first 3 ensembles for testing (speeds up processing)
    tot_prec_files = [f for f in tot_prec_files if '_e01_' in str(f) or '_e02_' in str(f) or '_e03_' in str(f)][:30]
    vmax_files = [f for f in vmax_files if '_e01_' in str(f) or '_e02_' in str(f) or '_e03_' in str(f)][:15]
    
    logger.info(f"Processing {len(tot_prec_files)} TOT_PREC files")
    logger.info(f"Processing {len(vmax_files)} VMAX_10M files")
    
    if not tot_prec_files and not vmax_files:
        logger.error("No GRIB files found for 2025-08-31T06:00")
        return False
    
    # Create mock run info
    run_info = {
        'run_time': '2025-08-31T06:00',
        'formatted_time': '2025-08-31T06%3A00',
        'ensembles': ['01', '02', '03'],
        'steps': ['PT000H00M', 'PT001H00M', 'PT002H00M']
    }
    
    # Setup configuration
    config = ProcessingConfig(num_runs=1, variables=['TOT_PREC', 'VMAX_10M'])
    orchestrator = PipelineOrchestrator(config)
    
    # Initialize grid
    grid_info = orchestrator.initialize_grid()
    target_location = orchestrator.setup_target_location(48.1486, 17.1077, "Bratislava")
    
    # Create files_by_variable dictionary
    files_by_variable = {
        'TOT_PREC': [str(f) for f in tot_prec_files],
        'VMAX_10M': [str(f) for f in vmax_files]
    }
    
    logger.info("Starting processing...")
    
    try:
        # Process the forecast run using existing files
        forecast_run = await orchestrator.process_forecast_run_from_files(
            run_info, files_by_variable
        )
        
        if forecast_run:
            logger.info(f"✅ Successfully processed forecast run {forecast_run.run_time}")
            logger.info(f"Variables: {list(forecast_run.variables.keys())}")
            
            for var_id, var_data in forecast_run.variables.items():
                logger.info(f"  {var_id}: {len(var_data.ensembles)} ensembles")
                if var_data.ensembles:
                    sample_ens = var_data.ensembles[0]
                    logger.info(f"    Sample ensemble {sample_ens.ensemble_id}: {len(sample_ens.times)} time steps")
            
            # Save outputs
            from utils.data_io import save_all_outputs
            
            output_dir = Path("data")
            save_all_outputs(forecast_run, output_dir, save_ensembles=True)
            
            logger.info("✅ Outputs saved successfully!")
            return True
        else:
            logger.error("❌ Processing failed")
            return False
            
    except Exception as e:
        logger.error(f"❌ Processing error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    print("Testing full processing pipeline...")
    success = asyncio.run(test_full_processing())
    print(f"Test result: {'PASSED' if success else 'FAILED'}")