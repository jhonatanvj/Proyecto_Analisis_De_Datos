# Workshop 2: Technical architecture and Bronze/Silver pipeline

| File | Content |
|---|---|
| [`report.tex`](report.tex) | Workshop report (LaTeX, build with `pdflatex report.tex` twice, or upload to Overleaf with the `figures/` folder) |
| `figures/` | Screenshots for the report: `airflow_dags.png`, `bronze_grid.png`, `silver_grid.png`, and `architecture_w2.png` rendered from [`architecture.mmd`](architecture.mmd). A missing image prints a placeholder box |
| [`design.md`](design.md) | Design agreed before the implementation: goals, scope, approaches considered |
| [`preprocessing.md`](preprocessing.md) | Every Silver cleaning rule, with the observed data issue that justifies it |
| [`architecture.mmd`](architecture.mmd) | Architecture diagram (Mermaid). Render it at https://mermaid.live or with `npx @mermaid-js/mermaid-cli -i architecture.mmd -o figures/architecture_w2.png` |
| `samples/bronze/` | Bronze JSON files produced by the `bronze_ingestion` DAG (GVP eruptions excluded, 11 MB) |
| `samples/silver/`, `samples/quarantine/` | Silver Parquet files and the quarantine file produced by `silver_processing` |

Refresh the samples from the current data lake before submitting:

```bash
python scripts/collect_w2_samples.py --per-source 3
```

How to run the pipeline: see the main [README](../README.md#getting-started-on-your-own-machine). Exploration notebook: [`notebooks/02_silver_exploration.ipynb`](../notebooks/02_silver_exploration.ipynb).
