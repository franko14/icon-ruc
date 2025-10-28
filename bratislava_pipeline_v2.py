#!/usr/bin/env python3
"""
Modular Weather Pipeline v2
===========================

New modular implementation of the Bratislava weather pipeline.
Uses separate modules for better maintainability and testability.
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path
from typing import List, Optional

# Import modular components
from utils.models import ProcessingConfig, TargetLocation
from utils.pipeline_orchestrator import PipelineOrchestrator, run_weather_pipeline


def setup_logging(level: str = 'INFO') -> None:
    """Setup logging configuration"""
    log_level = getattr(logging, level.upper())
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Modular Weather Pipeline v2 - Process ICON-D2-RUC-EPS ensemble forecasts',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process 4 runs for Bratislava (default)
  python bratislava_pipeline_v2.py

  # Process for different location
  python bratislava_pipeline_v2.py --lat 52.52 --lon 13.40 --location Berlin

  # Process for different location WITHOUT re-downloading (reuse existing GRIB files)
  python bratislava_pipeline_v2.py --lat 52.52 --lon 13.40 --location Berlin --skip-download

  # Process only precipitation with neighbor extraction
  python bratislava_pipeline_v2.py --variables TOT_PREC --extraction-method neighbors

  # Custom configuration
  python bratislava_pipeline_v2.py --runs 2 --max-workers 4 --output-dir /tmp/weather
        """
    )
    
    # Location parameters
    location_group = parser.add_argument_group('Location Settings')
    location_group.add_argument('--lat', type=float, default=48.1486,
                               help='Target latitude (default: Bratislava)')
    location_group.add_argument('--lon', type=float, default=17.1077, 
                               help='Target longitude (default: Bratislava)')
    location_group.add_argument('--location', type=str, default='Bratislava',
                               help='Location name (default: Bratislava)')
    
    # Processing parameters
    processing_group = parser.add_argument_group('Processing Settings')
    processing_group.add_argument('--runs', type=int, default=4,
                                 help='Number of forecast runs to process (default: 4)')
    processing_group.add_argument('--max-workers', type=int, default=8,
                                 help='Maximum concurrent workers (default: 8)')
    processing_group.add_argument('--variables', nargs='+', 
                                 choices=['TOT_PREC', 'VMAX_10M'],
                                 default=['TOT_PREC', 'VMAX_10M'],
                                 help='Variables to process (default: TOT_PREC VMAX_10M)')
    
    # Extraction parameters
    extraction_group = parser.add_argument_group('Extraction Settings')
    extraction_group.add_argument('--extraction-method', 
                                 choices=['single', 'neighbors'],
                                 default='single',
                                 help='Extraction method (default: single)')
    extraction_group.add_argument('--neighbor-radius', type=float, default=3.5,
                                 help='Neighbor radius in km (default: 3.5)')
    extraction_group.add_argument('--weighting-scheme',
                                 choices=['inverse_distance', 'center_weighted', 'gaussian', 'equal'],
                                 default='center_weighted',
                                 help='Weighting scheme for neighbors (default: center_weighted)')
    
    # Output parameters  
    output_group = parser.add_argument_group('Output Settings')
    output_group.add_argument('--output-dir', type=Path, default=None,
                             help='Output directory (default: ./data)')
    output_group.add_argument('--no-ensembles', action='store_true',
                             help='Skip saving individual ensemble files')
    output_group.add_argument('--no-statistics', action='store_true', 
                             help='Skip saving statistics files')
    output_group.add_argument('--save-netcdf', action='store_true',
                             help='Save NetCDF backup files')
    output_group.add_argument('--overwrite', action='store_true',
                             help='Overwrite existing outputs')
    
    # Other parameters
    other_group = parser.add_argument_group('Other Settings')
    other_group.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                           default='INFO', help='Logging level (default: INFO)')
    other_group.add_argument('--dry-run', action='store_true',
                           help='Show what would be processed without actually processing')
    other_group.add_argument('--skip-download', action='store_true',
                           help='Skip download and reuse existing GRIB files from data/raw/')
    
    return parser.parse_args()


def validate_arguments(args: argparse.Namespace) -> bool:
    """Validate parsed arguments"""
    # Validate coordinates
    if not (-90 <= args.lat <= 90):
        print(f"Error: Latitude {args.lat} out of range [-90, 90]")
        return False
        
    if not (-180 <= args.lon <= 180):
        print(f"Error: Longitude {args.lon} out of range [-180, 180]")
        return False
    
    # Validate other parameters
    if args.runs <= 0:
        print(f"Error: Number of runs must be positive, got {args.runs}")
        return False
        
    if args.max_workers <= 0:
        print(f"Error: Max workers must be positive, got {args.max_workers}")
        return False
        
    if args.neighbor_radius <= 0:
        print(f"Error: Neighbor radius must be positive, got {args.neighbor_radius}")
        return False
    
    return True


async def main():
    """Main pipeline execution"""
    # Parse arguments
    args = parse_arguments()
    
    # Setup logging
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)
    
    # Validate arguments
    if not validate_arguments(args):
        sys.exit(1)
    
    logger.info("=== Modular Weather Pipeline v2 ===")
    logger.info(f"Target: {args.location} ({args.lat:.4f}°N, {args.lon:.4f}°E)")
    logger.info(f"Variables: {', '.join(args.variables)}")
    logger.info(f"Runs: {args.runs}")
    logger.info(f"Extraction: {args.extraction_method}")
    
    if args.dry_run:
        logger.info("DRY RUN - No actual processing will occur")
        return
    
    try:
        # Create configuration
        config = ProcessingConfig(
            num_runs=args.runs,
            max_workers=args.max_workers,
            variables=args.variables,
            extraction_method=args.extraction_method,
            neighbor_radius_km=args.neighbor_radius,
            weighting_scheme=args.weighting_scheme,
            skip_download=args.skip_download,
            output_dir=str(args.output_dir) if args.output_dir else None,
            save_individual_ensembles=not args.no_ensembles,
            save_statistics=not args.no_statistics,
            save_netcdf_backup=args.save_netcdf,
            overwrite=args.overwrite
        )
        
        # Run pipeline
        orchestrator = PipelineOrchestrator(config)
        completed_runs = await orchestrator.run_pipeline(
            target_lat=args.lat,
            target_lon=args.lon,
            target_name=args.location,
            output_dir=args.output_dir
        )
        
        # Print summary
        logger.info("=== Pipeline Summary ===")
        logger.info(f"Successfully processed {len(completed_runs)} forecast runs")
        
        for run in completed_runs:
            logger.info(f"  Run {run.run_time}: {len(run.variables)} variables")
            for var_id, var_data in run.variables.items():
                logger.info(f"    {var_id}: {len(var_data.ensembles)} ensembles")
        
        if args.output_dir:
            logger.info(f"Output saved to: {args.output_dir}")
        else:
            logger.info("Output saved to: ./data")
        
        logger.info("Pipeline completed successfully! 🎉")
        
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        if args.log_level == 'DEBUG':
            import traceback
            traceback.print_exc()
        sys.exit(1)


# Convenience functions for programmatic usage
def run_bratislava_pipeline(num_runs: int = 4, 
                          variables: Optional[List[str]] = None,
                          **kwargs) -> List:
    """
    Convenience function to run pipeline for Bratislava.
    
    Args:
        num_runs: Number of forecast runs to process
        variables: Variables to process (default: TOT_PREC, VMAX_10M)
        **kwargs: Additional configuration options
    
    Returns:
        List of processed forecast runs
    """
    if variables is None:
        variables = ['TOT_PREC', 'VMAX_10M']
    
    return asyncio.run(run_weather_pipeline(
        target_lat=48.1486,
        target_lon=17.1077, 
        target_name="Bratislava",
        num_runs=num_runs,
        variables=variables,
        **kwargs
    ))


def run_custom_location_pipeline(lat: float, lon: float, 
                               name: str = "Custom Location",
                               **kwargs) -> List:
    """
    Convenience function to run pipeline for custom location.
    
    Args:
        lat: Target latitude
        lon: Target longitude
        name: Location name
        **kwargs: Additional configuration options
    
    Returns:
        List of processed forecast runs
    """
    return asyncio.run(run_weather_pipeline(
        target_lat=lat,
        target_lon=lon,
        target_name=name,
        **kwargs
    ))


if __name__ == '__main__':
    asyncio.run(main())