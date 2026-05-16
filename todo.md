- Complete and test extract.py [DONE]
- Complete and test transform.py [DONE]
- Complete and test create_db.py [DONE] & load.py Load Script & Create DuckDB Database [DONE]
- Check if the results are in the DB [DONE]
- Assemble the full pipeline in pipeline.py
- Schedule the pipeline in pipeline.py with prefect
- Add dbt Transformations
- Query & Analyse Data in a Notebook
- Dockerize & Document for Portfolio

# Improvements

- Create config file with path
- Create utils file with repeating functions
- Logger as decorator
- Create sql query file for creation table and loading tables
- Try and except to handle errors

Assemble the Full Pipeline",
phase: "Core Build",
difficulty: "🟡 Intermediate",
status: "backlog",
instructions: [
{ type: "text", content: "FILE: <code>src/pipeline.py</code> — Write the following code:" },
{ type: "code", content: 'from extract import fetch_weather\nfrom transform import clean_weather\nfrom load import load_to_duckdb\n\ndef run_pipeline(latitude=41.90, longitude=12.49, days=7):\n print("Step 1/3 — Extracting data from Open-Meteo...")\n df = fetch_weather(latitude, longitude, days)\n print(f" Fetched {len(df)} rows")\n print("Step 2/3 — Transforming data...")\n df = clean_weather(df)\n print(f" Clean rows: {len(df)}")\n print("Step 3/3 — Loading into DuckDB...")\n load_to_duckdb(df)\n print("Pipeline complete!")\n\nif __name__ == "__main__":\n run_pipeline()' },
{ type: "text", content: "Run the full pipeline:" },
{ type: "code", content: "poetry run python src/pipeline.py" },
{ type: "text", content: 'Commit: <code>git commit -m "feat: full ETL pipeline orchestration"</code>' },
],
