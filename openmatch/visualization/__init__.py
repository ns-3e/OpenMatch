"""
OpenMatch Visualization Module - Visualizes MDM results and metrics.
"""

from typing import Dict, List, Any, Optional
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import networkx as nx
from dataclasses import dataclass

@dataclass
class VisualizationConfig:
    """Configuration for visualization settings."""
    figure_size: tuple = (10, 6)
    style: str = "darkgrid"
    palette: str = "deep"
    font_scale: float = 1.2

class ResultsVisualizer:
    """Visualizes MDM results and metrics."""
    
    def __init__(self, config: Optional[VisualizationConfig] = None):
        """Initialize visualizer.
        
        Args:
            config: Optional visualization configuration
        """
        self.config = config or VisualizationConfig()
        sns.set_style(self.config.style)
        sns.set_palette(self.config.palette)
        plt.rcParams["figure.figsize"] = self.config.figure_size
        sns.set_context("notebook", font_scale=self.config.font_scale)
    
    def plot_match_distribution(self, results: Dict[str, Any]) -> None:
        """Plot distribution of match group sizes."""
        group_sizes = [len(group) for group in results.source_records.values()]
        
        plt.figure(figsize=(10, 6))
        sns.histplot(group_sizes, bins=20)
        plt.title('Distribution of Match Group Sizes')
        plt.xlabel('Group Size')
        plt.ylabel('Count')
        plt.savefig('match_distribution.png')
        plt.close()
    
    def plot_source_distribution(self, results: Dict[str, Any]) -> None:
        """Plot distribution of records by source."""
        sources = list(results.source_counts.keys())
        counts = list(results.source_counts.values())
        
        plt.figure(figsize=(10, 6))
        sns.barplot(x=sources, y=counts)
        plt.title('Record Distribution by Source')
        plt.xlabel('Source')
        plt.ylabel('Count')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig('source_distribution.png')
        plt.close()
    
    def plot_data_quality_metrics(self, results: Dict[str, Any]) -> None:
        """Plot data quality metrics."""
        metrics = list(results.quality_metrics.keys())
        scores = list(results.quality_metrics.values())
        
        plt.figure(figsize=(10, 6))
        sns.barplot(x=metrics, y=scores)
        plt.title('Data Quality Metrics')
        plt.xlabel('Metric')
        plt.ylabel('Score')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig('quality_metrics.png')
        plt.close()
    
    def plot_match_graph(self, matches: List[tuple]) -> None:
        """Plot graph of record matches.
        
        Args:
            matches: List of (record1_id, record2_id, score) tuples
        """
        G = nx.Graph()
        for r1, r2, score in matches:
            G.add_edge(r1, r2, weight=score)
        
        plt.figure(figsize=(12, 8))
        pos = nx.spring_layout(G)
        nx.draw(G, pos, with_labels=True, node_color='lightblue', 
                node_size=500, font_size=8, font_weight='bold')
        plt.title("Record Match Graph")
        plt.show() 