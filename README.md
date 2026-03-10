# Data Engineering Zoomcamp 2026 (Local Version)

Repositori ini berisi proyek dan latihan yang dikembangkan selama mengikuti program Data Engineering Zoomcamp 2026.

## 📂 Struktur Proyek

- **01_data_ingestion**: Modul awal fokus pada pengumpulan data (Web Scraping, API, dbt).
- **02_data_modeling**: Modul tentang pemodelan data dan konsep dimensional.
- **03_data_warehouse**: Modul fokus pada pembangunan Data Warehouse menggunakan DuckDB.
- **04_data_transformation**: Modul fokus pada transformasi data dan Analytics Engineering menggunakan dbt.
- **05_data_batch_processing**: Modul fokus pada processing data skala besar menggunakan apache spark.

## 🚀 Cara Menjalankan Proyek

### Prasyarat
Pastikan Anda telah menginstal dependensi yang diperlukan:
```bash
uv sync
```

### Menjalankan dbt
Untuk menjalankan model dbt:
```bash
uv run dbt run --select <nama_model>
```

### Menjalankan Visualisasi
Untuk menjalankan skrip visualisasi:
```bash
uv run python visualize_revenue.py
```

## 🛠️ Teknologi yang Digunakan
- **Python**: Bahasa pemrograman utama.
- **DuckDB**: Database untuk Data Warehouse.
- **dbt (data build tool)**: Alat untuk transformasi data.
- **Plotly**: Library visualisasi data.

## 📝 Catatan
Proyek ini adalah bagian dari kurikulum Data Engineering Zoomcamp 2026.
