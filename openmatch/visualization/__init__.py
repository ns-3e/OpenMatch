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
    
    def plot_match_distribution(self, results: Any) -> None:
        """Plot distribution of match scores.
        
        Args:
            results: MDM processing results
        """
        plt.figure()
        sns.histplot(results.match_scores, bins=30)
        plt.title("Distribution of Match Scores")
        plt.xlabel("Match Score")
        plt.ylabel("Count")
        plt.show()
    
    def plot_source_distribution(self, results: Any) -> None:
        """Plot distribution of records by source.
        
        Args:
            results: MDM processing results
        """
        plt.figure()
        source_counts = pd.Series(results.source_counts)
        source_counts.plot(kind="bar")
        plt.title("Record Distribution by Source")
        plt.xlabel("Source")
        plt.ylabel("Number of Records")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()
    
    def plot_data_quality_metrics(self, results: Any) -> None:
        """Plot data quality metrics.
        
        Args:
            results: MDM processing results
        """
        plt.figure()
        metrics = pd.Series(results.quality_metrics)
        metrics.plot(kind="bar")
        plt.title("Data Quality Metrics")
        plt.xlabel("Metric")
        plt.ylabel("Score")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()
    
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