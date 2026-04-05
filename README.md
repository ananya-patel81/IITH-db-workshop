# Robustness of Query Optimizers using IMDb JoB Benchmark
---
By: Ananya Patel

## Overview

This project studies the **robustness of PostgreSQL's query optimizer** using the IMDb **Join Order Benchmark (JoB)** dataset.

Database optimizers select execution plans based on estimated costs. However, small changes in **predicate selectivity** can cause the optimizer to switch plans. These switches do not always align with the **true optimal plan**, which can lead to performance degradation.

This project builds an experimental framework that:

- Detects **query plan switch points**
- Forces alternative execution plans
- Measures **true runtime performance**
- Computes **optimizer loss**
- Visualizes **plan regions and performance**

The experiments are automated using **Python scripts interacting with PostgreSQL**.

---

# Project Structure

robustness_project/

├── benchmark_runner.py  
├── switch_finder_ananya.py  
├── true_switch_finder.py  

├── query1.sql  
├── query2.sql  

├── results/  
│   └── robustness_data.csv  

├── plots/  

├── plot_results.py  
└── plot_plan_regions.py  

---

# Experimental Workflow

Query Template  
↓  
Plan Switch Detection  
↓  
Runtime Benchmarking  
↓  
Optimizer Robustness Measurement  
↓  
Visualization and Analysis  

