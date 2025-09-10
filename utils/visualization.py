"""
Visualization utilities for ICON-D2-RUC-EPS ensemble forecasts
"""
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.colors import LinearSegmentedColormap
import xarray as xr
from datetime import datetime, timedelta
import sys
sys.path.append('..')
from config import *

# Optional dependencies
try:
    import seaborn as sns
    HAS_SEABORN = True
except ImportError:
    HAS_SEABORN = False

class ForecastVisualizer:
    """
    Class for visualizing ICON-D2-RUC-EPS ensemble forecast data
    """
    
    def __init__(self, data=None):
        """
        Initialize visualizer with optional data.
        
        Args:
            data (xr.Dataset): Dataset with forecast data
        """
        self.data = data
        self.locations = LOCATIONS
        self.percentiles = PERCENTILES
        self.colors = self._setup_colors()
        
        # Set plotting style
        if HAS_SEABORN:
            try:
                plt.style.use('seaborn-v0_8')
                sns.set_palette("husl")
            except:
                # Fallback if seaborn style not available
                plt.style.use('default')
        else:
            plt.style.use('default')
    
    def _setup_colors(self):
        """Setup color schemes for different plot elements"""
        return {
            'ensemble_mean': '#1f77b4',
            'ensemble_median': '#ff7f0e', 
            'uncertainty_band': '#1f77b4',
            'individual_members': '#cccccc',
            'percentiles': ['#d62728', '#ff7f0e', '#2ca02c', '#ff7f0e', '#d62728'],
            'probability': '#e377c2',
            'background': '#f8f9fa'
        }
    
    def extract_point(self, lat, lon, method='nearest'):
        """
        Extract data for specific location.
        
        Args:
            lat (float): Latitude
            lon (float): Longitude
            method (str): Selection method for xarray
            
        Returns:
            xr.Dataset: Extracted data for the point
        """
        if self.data is None:
            raise ValueError("No data loaded in visualizer")
        
        point_data = self.data.sel(
            latitude=lat, 
            longitude=lon, 
            method=method
        )
        
        # Store actual coordinates
        actual_lat = float(point_data.latitude)
        actual_lon = float(point_data.longitude)
        
        # Add metadata
        point_data.attrs.update({
            'target_location': (lat, lon),
            'actual_location': (actual_lat, actual_lon),
            'distance_deg': np.sqrt((actual_lat - lat)**2 + (actual_lon - lon)**2)
        })
        
        return point_data
    
    def plot_ensemble_comparison(self, point_data, variable='mean', 
                                time_agg='1h', show_options=None, 
                                threshold=None, figsize=(15, 12)):
        """
        Create 2x2 comparison plot for 4 forecast runs.
        
        Args:
            point_data (xr.Dataset): Point data with run_time, ensemble, step
            variable (str): Variable type ('mean', 'median', 'p10', etc.)
            time_agg (str): Time aggregation period 
            show_options (list): Display options
            threshold (float): Threshold for probability calculations
            figsize (tuple): Figure size
            
        Returns:
            matplotlib.Figure: The created figure
        """
        if show_options is None:
            show_options = ['uncertainty']
            
        # Create 2x2 subplot
        fig, axes = plt.subplots(2, 2, figsize=figsize)
        fig.suptitle(f'Precipitation Forecast Comparison\n'
                    f'Location: {point_data.attrs.get("actual_location", "Unknown")} '
                    f'(Variable: {variable}, Aggregation: {time_agg})', 
                    fontsize=16)
        
        axes_flat = axes.flatten()
        
        # Plot each forecast run
        for idx, (ax, run_time) in enumerate(zip(axes_flat, point_data.run_time)):
            run_data = point_data.sel(run_time=run_time)
            
            # Calculate main variable
            main_values = self._calculate_variable(run_data, variable)
            
            # Convert step to datetime for better x-axis labels
            run_datetime = pd.to_datetime(run_time.values)
            forecast_times = [run_datetime + pd.Timedelta(hours=float(step)) 
                            for step in run_data.step]
            
            # Main plot
            ax.plot(forecast_times, main_values, 'b-', linewidth=2, 
                   label=f'{variable.title()}')
            
            # Add uncertainty bands if requested
            if 'uncertainty' in show_options:
                self._add_uncertainty_band(ax, run_data, forecast_times)
            
            # Add individual members if requested
            if 'members' in show_options:
                self._add_individual_members(ax, run_data, forecast_times)
            
            # Add ensemble spread if requested
            if 'spread' in show_options:
                self._add_ensemble_spread(ax, run_data, forecast_times)
            
            # Add probability threshold if requested
            if 'prob_threshold' in show_options and threshold is not None:
                self._add_probability_threshold(ax, run_data, forecast_times, threshold)
            
            # Formatting
            self._format_axis(ax, run_datetime, time_agg)
            ax.set_title(f"Run: {run_datetime.strftime('%Y-%m-%d %H:%M UTC')}")
            
            # Add statistics text box
            if 'stats' in show_options:
                self._add_statistics_box(ax, run_data)
        
        plt.tight_layout()
        return fig
    
    def _calculate_variable(self, run_data, variable):
        """Calculate requested variable from ensemble data"""
        if variable == 'mean':
            return run_data.tp.mean(dim='ensemble')
        elif variable == 'median':
            return run_data.tp.median(dim='ensemble') 
        elif variable.startswith('p') and variable[1:].isdigit():
            percentile = int(variable[1:])
            return run_data.tp.quantile(percentile/100, dim='ensemble')
        elif variable == 'min':
            return run_data.tp.min(dim='ensemble')
        elif variable == 'max':
            return run_data.tp.max(dim='ensemble')
        elif variable == 'std':
            return run_data.tp.std(dim='ensemble')
        else:
            # Default to mean
            return run_data.tp.mean(dim='ensemble')
    
    def _add_uncertainty_band(self, ax, run_data, forecast_times):
        """Add uncertainty band (10-90 percentiles)"""
        p10 = run_data.tp.quantile(0.1, dim='ensemble')
        p90 = run_data.tp.quantile(0.9, dim='ensemble')
        
        ax.fill_between(forecast_times, p10, p90, 
                       alpha=0.3, color=self.colors['uncertainty_band'],
                       label='10-90% range')
    
    def _add_individual_members(self, ax, run_data, forecast_times):
        """Add individual ensemble members"""
        for member in run_data.ensemble:
            member_data = run_data.tp.sel(ensemble=member)
            ax.plot(forecast_times, member_data, 
                   alpha=0.3, color=self.colors['individual_members'],
                   linewidth=0.5)
        
        # Add label only once
        ax.plot([], [], alpha=0.3, color=self.colors['individual_members'],
               linewidth=0.5, label='Individual members')
    
    def _add_ensemble_spread(self, ax, run_data, forecast_times):
        """Add ensemble spread (±1 std)"""
        mean = run_data.tp.mean(dim='ensemble')
        std = run_data.tp.std(dim='ensemble')
        
        ax.fill_between(forecast_times, mean-std, mean+std, 
                       alpha=0.2, color='gray', label='±1 std')
    
    def _add_probability_threshold(self, ax, run_data, forecast_times, threshold):
        """Add probability of exceeding threshold"""
        prob = (run_data.tp > threshold).mean(dim='ensemble') * 100
        
        ax2 = ax.twinx()
        ax2.plot(forecast_times, prob, 'r--', alpha=0.7,
                label=f'P(>{threshold} mm)')
        ax2.set_ylabel('Probability (%)', color='r')
        ax2.tick_params(axis='y', labelcolor='r')
        ax2.set_ylim(0, 100)
    
    def _format_axis(self, ax, run_datetime, time_agg):
        """Format axis labels and appearance"""
        ax.set_xlabel('Forecast Time')
        ax.set_ylabel(f'Precipitation ({time_agg})')
        ax.grid(True, alpha=0.3)
        ax.legend(loc='upper left', fontsize=8)
        
        # Format x-axis for dates
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H'))
        ax.xaxis.set_major_locator(mdates.HourLocator(interval=6))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
    
    def _add_statistics_box(self, ax, run_data):
        """Add statistics text box to plot"""
        mean_precip = float(run_data.tp.mean())
        max_precip = float(run_data.tp.max())
        std_precip = float(run_data.tp.std())
        
        stats_text = f'Mean: {mean_precip:.2f}\nMax: {max_precip:.2f}\nStd: {std_precip:.2f}'
        
        ax.text(0.02, 0.98, stats_text, transform=ax.transAxes,
                verticalalignment='top', fontsize=8,
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
    
    def plot_ensemble_evolution(self, point_data, figsize=(12, 8)):
        """
        Plot how ensemble forecast evolves across different runs.
        
        Args:
            point_data (xr.Dataset): Point data with multiple runs
            figsize (tuple): Figure size
            
        Returns:
            matplotlib.Figure: The created figure
        """
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=figsize, sharex=True)
        
        colors = plt.cm.viridis(np.linspace(0, 1, len(point_data.run_time)))
        
        for i, (run_time, color) in enumerate(zip(point_data.run_time, colors)):
            run_data = point_data.sel(run_time=run_time)
            run_datetime = pd.to_datetime(run_time.values)
            
            # Calculate valid time (run time + forecast step)
            valid_times = [run_datetime + pd.Timedelta(hours=float(step)) 
                          for step in run_data.step]
            
            mean_precip = run_data.tp.mean(dim='ensemble')
            std_precip = run_data.tp.std(dim='ensemble')
            
            label = run_datetime.strftime('%m-%d %H UTC')
            
            # Plot mean
            ax1.plot(valid_times, mean_precip, color=color, linewidth=2, label=label)
            
            # Plot spread
            ax2.plot(valid_times, std_precip, color=color, linewidth=2, label=label)
        
        ax1.set_ylabel('Mean Precipitation')
        ax1.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        ax1.grid(True, alpha=0.3)
        ax1.set_title('Ensemble Mean Evolution Across Runs')
        
        ax2.set_ylabel('Ensemble Spread (Std)')
        ax2.set_xlabel('Valid Time')
        ax2.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        ax2.grid(True, alpha=0.3)
        ax2.set_title('Ensemble Spread Evolution Across Runs')
        
        # Format x-axis
        for ax in [ax1, ax2]:
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H'))
            ax.xaxis.set_major_locator(mdates.HourLocator(interval=12))
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        
        plt.tight_layout()
        return fig
    
    def plot_probability_matrix(self, point_data, thresholds=None, figsize=(10, 8)):
        """
        Create probability matrix heatmap showing exceedance probabilities.
        
        Args:
            point_data (xr.Dataset): Point data with ensemble dimension
            thresholds (list): Precipitation thresholds to evaluate
            figsize (tuple): Figure size
            
        Returns:
            matplotlib.Figure: The created figure
        """
        if thresholds is None:
            thresholds = PRECIP_THRESHOLDS
        
        # Calculate probabilities for each run and threshold
        prob_matrix = []
        run_labels = []
        
        for run_time in point_data.run_time:
            run_data = point_data.sel(run_time=run_time)
            run_datetime = pd.to_datetime(run_time.values)
            run_labels.append(run_datetime.strftime('%m-%d %H UTC'))
            
            run_probs = []
            for threshold in thresholds:
                # Calculate maximum probability across all forecast steps
                prob_ts = (run_data.tp > threshold).mean(dim='ensemble')
                max_prob = float(prob_ts.max()) * 100
                run_probs.append(max_prob)
            
            prob_matrix.append(run_probs)
        
        # Create heatmap
        fig, ax = plt.subplots(figsize=figsize)
        
        im = ax.imshow(prob_matrix, cmap='YlOrRd', aspect='auto', vmin=0, vmax=100)
        
        # Set ticks and labels
        ax.set_xticks(range(len(thresholds)))
        ax.set_xticklabels([f'{t:.1f}' for t in thresholds])
        ax.set_yticks(range(len(run_labels)))
        ax.set_yticklabels(run_labels)
        
        ax.set_xlabel('Precipitation Threshold (mm/h)')
        ax.set_ylabel('Forecast Run')
        ax.set_title('Maximum Exceedance Probability by Run and Threshold')
        
        # Add text annotations
        for i in range(len(run_labels)):
            for j in range(len(thresholds)):
                text = ax.text(j, i, f'{prob_matrix[i][j]:.0f}%',
                             ha="center", va="center", color="black" if prob_matrix[i][j] < 50 else "white")
        
        # Add colorbar
        cbar = plt.colorbar(im, ax=ax)
        cbar.set_label('Probability (%)')
        
        plt.tight_layout()
        return fig
    
    def create_summary_table(self, point_data):
        """
        Create summary statistics table for all runs.
        
        Args:
            point_data (xr.Dataset): Point data with multiple runs
            
        Returns:
            pandas.DataFrame: Summary statistics table
        """
        summary_data = []
        
        for run_time in point_data.run_time:
            run_data = point_data.sel(run_time=run_time)
            run_datetime = pd.to_datetime(run_time.values)
            
            # Calculate statistics
            stats = {
                'Run': run_datetime.strftime('%Y-%m-%d %H:%M'),
                'Mean (mm/h)': float(run_data.tp.mean()),
                'Max (mm/h)': float(run_data.tp.max()),
                'P90 (mm/h)': float(run_data.tp.quantile(0.9, dim='ensemble').max()),
                'Std (mm/h)': float(run_data.tp.std(dim='ensemble').mean()),
                'Non-zero (%)': float((run_data.tp > 0.1).mean() * 100),
                'Heavy precip prob (%)': float((run_data.tp > 5.0).mean() * 100)
            }
            
            summary_data.append(stats)
        
        return pd.DataFrame(summary_data)
    
    def print_statistics(self, point_data, variable):
        """
        Print statistical summary for the point data.
        
        Args:
            point_data (xr.Dataset): Point data
            variable (str): Variable type being analyzed
        """
        print(f"\nSTATISTICAL SUMMARY - {variable.upper()}")
        print("=" * 50)
        
        # Location info
        target_loc = point_data.attrs.get('target_location', (None, None))
        actual_loc = point_data.attrs.get('actual_location', (None, None))
        distance = point_data.attrs.get('distance_deg', None)
        
        print(f"Target location: {target_loc[0]:.3f}°N, {target_loc[1]:.3f}°E")
        print(f"Actual grid point: {actual_loc[0]:.3f}°N, {actual_loc[1]:.3f}°E")
        print(f"Distance: {distance:.4f}°")
        
        # Data dimensions
        print(f"\nData dimensions:")
        print(f"  Runs: {len(point_data.run_time)}")
        print(f"  Ensembles: {len(point_data.ensemble)}")
        print(f"  Time steps: {len(point_data.step)}")
        
        # Overall statistics
        all_data = point_data.tp
        print(f"\nOverall statistics:")
        print(f"  Min: {float(all_data.min()):.4f}")
        print(f"  Max: {float(all_data.max()):.4f}")
        print(f"  Mean: {float(all_data.mean()):.4f}")
        print(f"  Std: {float(all_data.std()):.4f}")
        print(f"  Non-zero: {float((all_data > 0.1).mean() * 100):.1f}%")
    
    def save_plot(self, fig, filename, dpi=150, bbox_inches='tight'):
        """
        Save plot to file.
        
        Args:
            fig (matplotlib.Figure): Figure to save
            filename (str): Output filename
            dpi (int): Resolution in DPI
            bbox_inches (str): Bounding box setting
        """
        output_path = OUTPUTS_DIR / filename
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        fig.savefig(output_path, dpi=dpi, bbox_inches=bbox_inches)
        print(f"Plot saved: {output_path}")
    
    def create_interactive_widgets(self):
        """
        Create ipywidgets for interactive plotting.
        
        Returns:
            dict: Dictionary of widgets
        """
        try:
            import ipywidgets as widgets
        except ImportError:
            print("ipywidgets not available - install with: pip install ipywidgets")
            return None
        
        # Location selection widget
        location_options = [(name, coords) for name, coords in self.locations.items()]
        location_options.append(('Custom...', {'lat': None, 'lon': None}))
        
        widgets_dict = {
            'location': widgets.Dropdown(
                options=location_options,
                value=location_options[0][1],
                description='Location:'
            ),
            
            'variable': widgets.Dropdown(
                options=[
                    ('Ensemble Mean', 'mean'),
                    ('Ensemble Median', 'median'),
                    ('10th Percentile', 'p10'),
                    ('25th Percentile', 'p25'),
                    ('75th Percentile', 'p75'),
                    ('90th Percentile', 'p90'),
                    ('Ensemble Min', 'min'),
                    ('Ensemble Max', 'max'),
                    ('Ensemble Spread', 'std'),
                ],
                value='mean',
                description='Variable:'
            ),
            
            'time_agg': widgets.Dropdown(
                options=list(TIME_AGGREGATIONS.keys()),
                value='1h',
                description='Aggregation:'
            ),
            
            'display_options': widgets.SelectMultiple(
                options=[
                    ('Uncertainty bands', 'uncertainty'),
                    ('Individual members', 'members'),
                    ('Ensemble spread', 'spread'),
                    ('Statistics', 'stats'),
                    ('Probability threshold', 'prob_threshold')
                ],
                value=['uncertainty'],
                description='Display:'
            ),
            
            'threshold': widgets.FloatSlider(
                value=1.0,
                min=0.0,
                max=10.0,
                step=0.1,
                description='Threshold (mm/h):'
            )
        }
        
        return widgets_dict