#!/usr/bin/env python3
"""
Quick test of the modular pipeline with minimal data
"""

import asyncio
import logging
from pathlib import Path

from utils.models import ProcessingConfig
from utils.pipeline_orchestrator import PipelineOrchestrator

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def test_minimal_pipeline():
    """Test with minimal data to verify structure creation"""
    logger.info("=== Testing Minimal Modular Pipeline ===")
    
    try:
        # Configuration for ultra-minimal test
        config = ProcessingConfig(
            num_runs=1,  
            variables=['TOT_PREC'],  
            extraction_method='single',
            save_individual_ensembles=True,
            save_statistics=True,
            output_dir='./data_test'
        )
        
        orchestrator = PipelineOrchestrator(config)
        
        # Initialize components
        orchestrator.initialize_grid()
        orchestrator.setup_target_location(48.1486, 17.1077, "Bratislava_Test")
        
        # Test discovery
        runs = await orchestrator.discover_forecast_runs()
        if not runs:
            logger.error("No runs discovered")
            return False
            
        logger.info(f"Found run: {runs[0]['run_time']}")
        
        # Test download format - just check first few files
        run_info = runs[0]
        logger.info("Testing download data structure...")
        
        # Just test a couple files to verify format
        test_download_list = [
            (run_info['run_time'], '01', 'PT000H05M'),
            (run_info['run_time'], '01', 'PT000H10M'), 
            (run_info['run_time'], '02', 'PT000H05M'),
        ]
        
        logger.info(f"Testing download of {len(test_download_list)} files...")
        from utils.download import smart_batch_download
        
        test_files = smart_batch_download(
            test_download_list,
            max_workers=2,
            max_concurrent=2
        )
        
        logger.info(f"Downloaded {len(test_files)} test files")
        
        if test_files:
            logger.info("✅ Download system working!")
            logger.info(f"Sample file: {test_files[0]}")
            
            # Quick test of data extraction
            from utils.extraction import extract_point_from_grib
            from utils.models import get_variable_config
            
            sample_file = test_files[0]
            variable = get_variable_config('TOT_PREC')
            
            logger.info("Testing data extraction...")
            value = extract_point_from_grib(
                sample_file, orchestrator.target_location, 
                orchestrator.grid_info, variable
            )
            
            if value is not None:
                logger.info(f"✅ Extracted value: {value:.4f} {variable.unit}")
                
                # Test creating simple ensemble data structure
                from utils.models import EnsembleMember
                
                test_ensemble = EnsembleMember(
                    ensemble_id="01",
                    times=["2025-08-31T05:05:00", "2025-08-31T05:10:00"],
                    values=[value, value * 1.1]
                )
                
                logger.info("✅ Created ensemble data structure")
                
                # Test output structure
                from utils.data_io import OutputManager, save_json
                
                output_manager = OutputManager('./data_test')
                test_dir = output_manager.weather_dir / 'test_forecast'
                test_dir.mkdir(exist_ok=True)
                
                # Save test ensemble file
                test_file = test_dir / 'TOT_PREC_ensemble_01.json'
                save_json(test_ensemble.to_dict(), test_file)
                
                logger.info(f"✅ Created test output: {test_file}")
                
                # Verify file exists and has content
                if test_file.exists() and test_file.stat().st_size > 0:
                    logger.info("✅ Output file created successfully!")
                    return True
            
        logger.warning("❌ Some tests failed")
        return False
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    """Run minimal test"""
    logger.info("🧪 Starting Quick Pipeline Test")
    
    success = await test_minimal_pipeline()
    
    if success:
        logger.info("🎉 Quick test passed! The modular pipeline infrastructure works.")
    else:
        logger.error("❌ Quick test failed")


if __name__ == '__main__':
    asyncio.run(main())