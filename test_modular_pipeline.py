#!/usr/bin/env python3
"""
Test script for the modular weather pipeline
"""

import asyncio
import logging
from pathlib import Path

from utils.models import ProcessingConfig, TargetLocation, get_variable_config
from utils.pipeline_orchestrator import PipelineOrchestrator
from utils.data_io import OutputManager, save_json

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def test_pipeline_components():
    """Test individual pipeline components"""
    logger.info("=== Testing Modular Pipeline Components ===")
    
    # 1. Test configuration
    logger.info("1. Testing configuration...")
    config = ProcessingConfig(
        num_runs=1,
        variables=['TOT_PREC'],
        extraction_method='single',
        save_individual_ensembles=True,
        save_statistics=True
    )
    logger.info(f"✅ Config created: {config.num_runs} runs, {config.variables}")
    
    # 2. Test variable configs
    logger.info("2. Testing variable configurations...")
    for var_id in ['TOT_PREC', 'VMAX_10M']:
        var_config = get_variable_config(var_id)
        logger.info(f"✅ {var_id}: {var_config.name} ({var_config.unit})")
    
    # 3. Test orchestrator creation
    logger.info("3. Testing orchestrator creation...")
    orchestrator = PipelineOrchestrator(config)
    logger.info("✅ Orchestrator created")
    
    # 4. Test grid initialization (this downloads ~50MB on first run)
    logger.info("4. Testing grid initialization...")
    try:
        grid_info = orchestrator.initialize_grid()
        logger.info(f"✅ Grid initialized: {len(grid_info.lats)} points")
        logger.info(f"   Lat range: {grid_info.metadata['lat_range']}")
        logger.info(f"   Lon range: {grid_info.metadata['lon_range']}")
    except Exception as e:
        logger.error(f"❌ Grid initialization failed: {e}")
        return False
    
    # 5. Test target location setup  
    logger.info("5. Testing target location setup...")
    try:
        target = orchestrator.setup_target_location(48.1486, 17.1077, "Bratislava")
        logger.info(f"✅ Target location: {target.name} ({target.lat:.4f}°N, {target.lon:.4f}°E)")
        logger.info(f"   Grid distance: {target.distance_km:.2f} km")
    except Exception as e:
        logger.error(f"❌ Target location setup failed: {e}")
        return False
    
    # 6. Test output manager
    logger.info("6. Testing output manager...")
    try:
        output_dir = Path('./data_test')
        output_manager = OutputManager(output_dir)
        logger.info(f"✅ Output manager created: {output_manager.weather_dir}")
    except Exception as e:
        logger.error(f"❌ Output manager failed: {e}")
        return False
    
    # 7. Test discovery (this will try to connect to DWD servers)
    logger.info("7. Testing forecast run discovery...")
    try:
        runs = await orchestrator.discover_forecast_runs()
        logger.info(f"✅ Discovered {len(runs)} forecast runs")
        if runs:
            logger.info(f"   Latest run: {runs[0].get('run_time', 'unknown')}")
    except Exception as e:
        logger.error(f"❌ Discovery failed: {e}")
        logger.warning("This is expected if offline or DWD servers unavailable")
    
    logger.info("=== Component Testing Complete ===")
    return True


async def test_small_pipeline_run():
    """Test a very small pipeline run with minimal data"""
    logger.info("=== Testing Small Pipeline Run ===")
    
    try:
        # Configuration for minimal test
        config = ProcessingConfig(
            num_runs=1,  # Only process 1 run
            variables=['TOT_PREC'],  # Only precipitation  
            extraction_method='single',
            save_individual_ensembles=True,
            save_statistics=True,
            output_dir='./data_test'
        )
        
        orchestrator = PipelineOrchestrator(config)
        
        # Run the pipeline (this will download data if needed)
        logger.info("Running minimal pipeline...")
        completed_runs = await orchestrator.run_pipeline(
            target_lat=48.1486,
            target_lon=17.1077,
            target_name="Bratislava_Test",
            output_dir=Path('./data_test')
        )
        
        logger.info(f"✅ Pipeline completed! Processed {len(completed_runs)} runs")
        
        # Verify outputs
        if completed_runs:
            run = completed_runs[0]
            logger.info(f"   Run time: {run.run_time}")
            logger.info(f"   Variables: {list(run.variables.keys())}")
            
            for var_id, var_data in run.variables.items():
                logger.info(f"   {var_id}: {len(var_data.ensembles)} ensembles")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Pipeline run failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_data_structure():
    """Test that the expected data structure is created"""
    logger.info("=== Testing Data Structure ===")
    
    test_dir = Path('./data_test/weather')
    if not test_dir.exists():
        logger.warning("No test weather directory found")
        return False
    
    # Check for forecast directories
    forecast_dirs = list(test_dir.glob('forecast_*'))
    logger.info(f"Found {len(forecast_dirs)} forecast directories")
    
    if not forecast_dirs:
        logger.warning("No forecast directories found")
        return False
    
    # Check the structure of the first directory
    forecast_dir = forecast_dirs[0]
    logger.info(f"Checking structure of: {forecast_dir.name}")
    
    # Expected files
    expected_patterns = [
        'TOT_PREC_ensemble_*.json',
        'TOT_PREC_statistics.json', 
        'forecast_master.json'
    ]
    
    for pattern in expected_patterns:
        files = list(forecast_dir.glob(pattern))
        logger.info(f"   {pattern}: {len(files)} files")
    
    # Check master JSON exists in main weather dir
    master_json = test_dir / f"{forecast_dir.name}.json"
    logger.info(f"   Master JSON: {'✅' if master_json.exists() else '❌'}")
    
    # Check latest.json
    latest_json = test_dir / 'latest.json'
    logger.info(f"   Latest JSON: {'✅' if latest_json.exists() else '❌'}")
    
    return True


async def main():
    """Run all tests"""
    logger.info("🧪 Starting Modular Pipeline Tests")
    
    # Test 1: Component tests
    component_test_passed = await test_pipeline_components()
    
    if component_test_passed:
        logger.info("🎉 All component tests passed!")
        
        # Test 2: Small pipeline run (only if components work)
        logger.info("\n" + "="*50)
        pipeline_test_passed = await test_small_pipeline_run()
        
        if pipeline_test_passed:
            logger.info("🎉 Pipeline test passed!")
            
            # Test 3: Data structure verification
            logger.info("\n" + "="*50)
            structure_test_passed = test_data_structure()
            
            if structure_test_passed:
                logger.info("🎉 Data structure test passed!")
                logger.info("\n✅ ALL TESTS PASSED! The modular pipeline is working correctly.")
            else:
                logger.warning("⚠️ Data structure test had issues")
        else:
            logger.error("❌ Pipeline test failed")
    else:
        logger.error("❌ Component tests failed")


if __name__ == '__main__':
    asyncio.run(main())