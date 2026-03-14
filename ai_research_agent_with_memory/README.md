# Human-in-the-Loop Research Agent

A Python research agent with:

- Tavily web search
- Chroma persistent semantic memory
- project-based isolation using `project_id`
- local `projects.json` registry
- human feedback loop for refining research
- Streamlit user interface

## Features

- Start a new research project
- Resume a saved project by name
- Rename or delete saved projects
- Prevent cross-project contamination with project-scoped memory
- Generate structured research reports
- Persist memory locally with Chroma

## Tech Stack

- Python
- LangGraph
- OpenAI
- Tavily
- ChromaDB
- Streamlit

## Project Structure

```text
research-agent-ui/
├── app.py
├── src/
├── ui/
├── tests/
├── docs/
├── .env.example
├── requirements.txt
└── README.md