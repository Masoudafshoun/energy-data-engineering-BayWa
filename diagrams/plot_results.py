from __future__ import annotations
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from src.config.settings import Settings
from src.utils.spark_session import get_spark_session

def plot_daily_power_mix(df: pd.DataFrame):
    """Generates a stacked area chart for Use Case 1."""
    # Filter out categories that typically contain negative values (like trading/consumption)
    # to allow the stacked area chart to render correctly.
    exclude_cols = ['Cross border electricity trading', 'Pumped storage consumption']
    
    # Create the pivot
    pivot_df = df.pivot(index='date', columns='production_type', values='daily_total_mwh')
    
    # Filter to only include columns that are strictly positive for the stack
    plot_df = pivot_df.drop(columns=[c for c in exclude_cols if c in pivot_df.columns])
    
    plt.figure(figsize=(12, 6))
    plot_df.plot(kind='area', stacked=True, alpha=0.7, ax=plt.gca())
    
    plt.title("Germany: Trend of Daily Public Net Electricity Production (Positive Generation)")
    plt.ylabel("Production (MWh)")
    plt.xlabel("Date")
    plt.legend(loc='upper left', bbox_to_anchor=(1, 1), fontsize='small')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig("daily_power_mix_trend.png")
    print("✅ Saved: daily_power_mix_trend.png")

def plot_price_wind_correlation(df: pd.DataFrame):
    """Generates a scatter plot with a trendline for Use Case 2."""
    plt.figure(figsize=(10, 6))
    sns.regplot(data=df, x='offshore_wind_mwh', y='avg_price_eur_mwh', 
                scatter_kws={'alpha':0.5}, line_kws={'color':'red'})
    
    plt.title("Correlation: Daily Price vs. Offshore Wind Power")
    plt.xlabel("Offshore Wind Production (MWh)")
    plt.ylabel("Avg Daily Price (€/MWh)")
    plt.grid(True, linestyle=':', alpha=0.6)
    plt.tight_layout()
    plt.savefig("price_offshore_correlation.png")
    print("✅ Saved: price_offshore_correlation.png")

def main():
    s = Settings.from_env()
    spark = get_spark_session("visualizer")
    
    # Load Gold Tables and convert to Pandas for plotting
    power_mix_path = f"{s.data_root}/gold/power_mix_daily"
    correlation_path = f"{s.data_root}/gold/price_vs_offshore_daily"
    
    # Use Case 1 Data
    df_mix = spark.read.format("delta").load(power_mix_path).toPandas()
    df_mix['date'] = pd.to_datetime(df_mix['date'])
    
    # Use Case 2 Data
    df_corr = spark.read.format("delta").load(correlation_path).toPandas()
    df_corr['date'] = pd.to_datetime(df_corr['date'])

    # Generate Plots
    plot_daily_power_mix(df_mix)
    plot_price_wind_correlation(df_corr)
    
    spark.stop()

if __name__ == "__main__":
    main()
