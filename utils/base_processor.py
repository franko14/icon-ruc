"""
Base processor class for weather variables
"""
from abc import ABC, abstractmethod
import xarray as xr
import numpy as np
from typing import Dict, List, Optional


class BaseVariableProcessor(ABC):
    """
    Abstract base class for weather variable processors.
    Defines common interface for processing different weather variables.
    """
    
    def __init__(self, variable_id: str):
        self.variable_id = variable_id
        self.variable_name = self.get_variable_name()
        self.units = self.get_units()
        self.processing_type = self.get_processing_type()
    
    @abstractmethod
    def get_variable_name(self) -> str:
        """Return the main variable name in the dataset"""
        pass
    
    @abstractmethod
    def get_units(self) -> str:
        """Return the units for this variable"""
        pass
    
    @abstractmethod
    def get_processing_type(self) -> str:
        """Return processing type: 'accumulated' or 'instantaneous'"""
        pass
    
    @abstractmethod
    def calculate_ensemble_statistics(self, data: xr.Dataset, ensemble_dim: str = 'ensemble') -> xr.Dataset:
        """Calculate ensemble statistics specific to this variable"""
        pass
    
    @abstractmethod
    def calculate_percentiles(self, data: xr.Dataset, ensemble_dim: str = 'ensemble') -> xr.Dataset:
        """Calculate percentiles for this variable"""
        pass
    
    @abstractmethod
    def probability_exceedance(self, data: xr.Dataset, thresholds: Optional[List[float]] = None, 
                              ensemble_dim: str = 'ensemble') -> xr.Dataset:
        """Calculate probability of exceeding thresholds"""
        pass
    
    @abstractmethod
    def create_summary(self, data: xr.Dataset) -> Dict:
        """Create summary statistics for this variable"""
        pass
    
    @abstractmethod
    def print_summary(self, summary: Dict):
        """Print formatted summary"""
        pass
    
    def validate_data(self, data: xr.Dataset) -> bool:
        """
        Validate that dataset contains the required variable.
        
        Args:
            data: xarray Dataset
            
        Returns:
            bool: True if valid, False otherwise
        """
        var_name = self.get_variable_name()
        if var_name not in data.data_vars:
            print(f"Warning: Required variable '{var_name}' not found in dataset")
            print(f"Available variables: {list(data.data_vars.keys())}")
            return False
        return True
    
    def add_metadata(self, data: xr.Dataset) -> xr.Dataset:
        """
        Add standard metadata to dataset.
        
        Args:
            data: xarray Dataset
            
        Returns:
            xr.Dataset: Dataset with added metadata
        """
        result = data.copy()
        
        # Add global attributes
        result.attrs.update({
            'variable_id': self.variable_id,
            'processing_type': self.processing_type,
            'processor_class': self.__class__.__name__,
            'created_at': str(np.datetime64('now'))
        })
        
        return result
    
    def process_complete_pipeline(self, data: xr.Dataset, 
                                 ensemble_dim: str = 'ensemble',
                                 calculate_stats: bool = True,
                                 calculate_percentiles: bool = True,
                                 calculate_probabilities: bool = True,
                                 thresholds: Optional[List[float]] = None) -> xr.Dataset:
        """
        Run complete processing pipeline for this variable.
        
        Args:
            data: Input dataset
            ensemble_dim: Name of ensemble dimension
            calculate_stats: Whether to calculate ensemble statistics
            calculate_percentiles: Whether to calculate percentiles
            calculate_probabilities: Whether to calculate exceedance probabilities
            thresholds: Custom thresholds for probability calculation
            
        Returns:
            xr.Dataset: Fully processed dataset
        """
        print(f"\n{'='*50}")
        print(f"Processing {self.variable_id} ({self.__class__.__name__})")
        print(f"{'='*50}")
        
        # Validate input data
        if not self.validate_data(data):
            return data
        
        result = data.copy()
        
        # Add metadata
        result = self.add_metadata(result)
        
        # Calculate ensemble statistics
        if calculate_stats:
            result = self.calculate_ensemble_statistics(result, ensemble_dim)
        
        # Calculate percentiles  
        if calculate_percentiles:
            result = self.calculate_percentiles(result, ensemble_dim)
        
        # Calculate exceedance probabilities
        if calculate_probabilities:
            result = self.probability_exceedance(result, thresholds, ensemble_dim)
        
        # Create and print summary
        summary = self.create_summary(result)
        self.print_summary(summary)
        
        print(f"✅ {self.variable_id} processing completed")
        return result


class MultiVariableProcessor:
    """
    Processor that can handle multiple weather variables simultaneously.
    """
    
    def __init__(self):
        self.processors = {}
    
    def register_processor(self, variable_id: str, processor: BaseVariableProcessor):
        """Register a processor for a specific variable"""
        self.processors[variable_id] = processor
        print(f"Registered processor for {variable_id}: {processor.__class__.__name__}")
    
    def get_processor(self, variable_id: str) -> Optional[BaseVariableProcessor]:
        """Get processor for a specific variable"""
        return self.processors.get(variable_id)
    
    def process_variable(self, variable_id: str, data: xr.Dataset, **kwargs) -> xr.Dataset:
        """Process a single variable using its registered processor"""
        processor = self.get_processor(variable_id)
        if processor is None:
            print(f"No processor registered for variable: {variable_id}")
            return data
        
        return processor.process_complete_pipeline(data, **kwargs)
    
    def process_multiple_variables(self, data_dict: Dict[str, xr.Dataset], **kwargs) -> Dict[str, xr.Dataset]:
        """
        Process multiple variables, each with its own dataset.
        
        Args:
            data_dict: Dictionary mapping variable_id to dataset
            **kwargs: Arguments passed to each processor
            
        Returns:
            Dict[str, xr.Dataset]: Processed datasets for each variable
        """
        results = {}
        
        for variable_id, data in data_dict.items():
            print(f"\n🔄 Processing variable: {variable_id}")
            results[variable_id] = self.process_variable(variable_id, data, **kwargs)
        
        return results
    
    def create_combined_summary(self, data_dict: Dict[str, xr.Dataset]) -> Dict[str, Dict]:
        """Create summaries for all variables"""
        summaries = {}
        
        for variable_id, data in data_dict.items():
            processor = self.get_processor(variable_id)
            if processor and processor.validate_data(data):
                summaries[variable_id] = processor.create_summary(data)
        
        return summaries
    
    def print_combined_summary(self, summaries: Dict[str, Dict]):
        """Print formatted summary for all variables"""
        print(f"\n{'='*60}")
        print("MULTI-VARIABLE PROCESSING SUMMARY")
        print(f"{'='*60}")
        
        for variable_id, summary in summaries.items():
            processor = self.get_processor(variable_id)
            if processor:
                print(f"\n--- {variable_id} ---")
                processor.print_summary(summary)
    
    def get_available_processors(self) -> List[str]:
        """Get list of available variable processors"""
        return list(self.processors.keys())


# Factory function for creating processors
def create_processor(variable_id: str) -> Optional[BaseVariableProcessor]:
    """
    Factory function to create appropriate processor for a variable.
    
    Args:
        variable_id: Variable identifier (e.g., 'TOT_PREC', 'VMAX_10M')
        
    Returns:
        BaseVariableProcessor: Appropriate processor instance or None
    """
    if variable_id == 'TOT_PREC':
        from .precipitation import PrecipitationProcessor
        return PrecipitationProcessorAdapter('TOT_PREC')
    elif variable_id == 'VMAX_10M':
        from .wind_speed import WindSpeedProcessor
        return WindSpeedProcessorAdapter('VMAX_10M')
    else:
        print(f"Unknown variable_id: {variable_id}")
        return None


# Adapter classes to make existing processors compatible with base interface
class PrecipitationProcessorAdapter(BaseVariableProcessor):
    """Adapter to make PrecipitationProcessor compatible with BaseVariableProcessor"""
    
    def __init__(self, variable_id: str):
        from .precipitation import PrecipitationProcessor
        super().__init__(variable_id)
        self.processor = PrecipitationProcessor()
    
    def get_variable_name(self) -> str:
        return 'tp'
    
    def get_units(self) -> str:
        return 'mm/h'
    
    def get_processing_type(self) -> str:
        return 'accumulated'
    
    def calculate_ensemble_statistics(self, data: xr.Dataset, ensemble_dim: str = 'ensemble') -> xr.Dataset:
        return self.processor.calculate_ensemble_statistics(data, ensemble_dim)
    
    def calculate_percentiles(self, data: xr.Dataset, ensemble_dim: str = 'ensemble') -> xr.Dataset:
        return self.processor.calculate_percentiles(data, ensemble_dim)
    
    def probability_exceedance(self, data: xr.Dataset, thresholds: Optional[List[float]] = None, 
                              ensemble_dim: str = 'ensemble') -> xr.Dataset:
        return self.processor.probability_exceedance(data, thresholds, ensemble_dim)
    
    def create_summary(self, data: xr.Dataset) -> Dict:
        return self.processor.create_precipitation_summary(data)
    
    def print_summary(self, summary: Dict):
        return self.processor.print_summary(summary)


class WindSpeedProcessorAdapter(BaseVariableProcessor):
    """Adapter to make WindSpeedProcessor compatible with BaseVariableProcessor"""
    
    def __init__(self, variable_id: str):
        from .wind_speed import WindSpeedProcessor
        super().__init__(variable_id)
        self.processor = WindSpeedProcessor()
    
    def get_variable_name(self) -> str:
        return 'vmax_10m'
    
    def get_units(self) -> str:
        return 'm/s'
    
    def get_processing_type(self) -> str:
        return 'instantaneous'
    
    def calculate_ensemble_statistics(self, data: xr.Dataset, ensemble_dim: str = 'ensemble') -> xr.Dataset:
        return self.processor.calculate_wind_statistics(data, ensemble_dim)
    
    def calculate_percentiles(self, data: xr.Dataset, ensemble_dim: str = 'ensemble') -> xr.Dataset:
        return self.processor.calculate_percentiles(data, ensemble_dim)
    
    def probability_exceedance(self, data: xr.Dataset, thresholds: Optional[List[float]] = None, 
                              ensemble_dim: str = 'ensemble') -> xr.Dataset:
        return self.processor.probability_exceedance(data, thresholds, ensemble_dim)
    
    def create_summary(self, data: xr.Dataset) -> Dict:
        return self.processor.create_wind_summary(data)
    
    def print_summary(self, summary: Dict):
        return self.processor.print_summary(summary)