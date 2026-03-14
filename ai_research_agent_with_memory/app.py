import streamlit as st

from src.config import STREAMLIT_APP_TITLE, STREAMLIT_LAYOUT
from src.registry import (
    list_projects,
    upsert_project,
    get_project_by_name,
    rename_project,
    delete_project_record,
)
from src.vector_store import (
    load_project_memory,
    derive_visited_urls,
    reset_memory_for_project,
    get_project_memory_count,
)
from src.research import web_research
from src.report import (
    generate_analysis,
    is_analysis_approved,
    generate_final_report,
)
from src.utils import format_source_label, make_memory_id, truncate_text


# ======================================================
# PAGE SETUP
# ======================================================

st.set_page_config(
    page_title=STREAMLIT_APP_TITLE,
    layout=STREAMLIT_LAYOUT,
)

st.title("Human-in-the-Loop Research Agent")
st.caption("Tavily + OpenAI + Chroma + project memory + human refinement")


# ======================================================
# SESSION STATE
# ======================================================

def init_session_state() -> None:
    defaults = {
        "project_name": "",
        "project_id": "",
        "topic": "",
        "human_guidance": "",
        "research_results": [],
        "visited_urls": [],
        "analysis": "",
        "approved": False,
        "report": "",
    }

    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


init_session_state()


# ======================================================
# HELPERS
# ======================================================

def load_project_into_session(project_name: str) -> bool:
    project = get_project_by_name(project_name)
    if not project:
        return False

    project_id = project["project_id"]
    entries = load_project_memory(project_id)
    visited_urls = derive_visited_urls(entries)

    st.session_state.project_name = project["project_name"]
    st.session_state.project_id = project["project_id"]
    st.session_state.topic = project["topic"]
    st.session_state.research_results = entries
    st.session_state.visited_urls = visited_urls
    st.session_state.analysis = ""
    st.session_state.approved = False
    st.session_state.report = ""
    return True


def clear_workflow_outputs() -> None:
    st.session_state.analysis = ""
    st.session_state.approved = False
    st.session_state.report = ""


def ensure_active_project() -> bool:
    if not st.session_state.project_name or not st.session_state.project_id:
        st.warning("Select or create a project first.")
        return False
    return True


def current_iteration() -> int:
    if not st.session_state.research_results:
        return 1
    return max(entry.get("iteration", 0) for entry in st.session_state.research_results) + 1


def add_research_result(summary: str, sources: list, guidance: str) -> None:
    iteration = current_iteration()

    entry = {
        "topic": st.session_state.topic,
        "summary": summary,
        "sources": sources,
        "iteration": iteration,
        "human_feedback_context": guidance,
        "memory_id": make_memory_id(
            f"{st.session_state.project_id}::{iteration}::{summary}"
        ),
    }

    from src.vector_store import add_to_memory  # local import avoids circular issues

    memory_id = add_to_memory(
        project_id=st.session_state.project_id,
        topic=st.session_state.topic,
        entry=entry,
    )
    entry["memory_id"] = memory_id

    st.session_state.research_results = st.session_state.research_results + [entry]
    new_urls = [s.get("url", "") for s in sources if s.get("url")]
    st.session_state.visited_urls = list(
        dict.fromkeys(st.session_state.visited_urls + new_urls)
    )


# ======================================================
# SIDEBAR
# ======================================================

with st.sidebar:
    st.header("Projects")

    projects = list_projects()
    project_names = [p["project_name"] for p in projects]

    selected_project = st.selectbox(
        "Resume existing project",
        options=[""] + project_names,
        index=0,
    )

    if st.button("Load Project", use_container_width=True):
        if selected_project:
            if load_project_into_session(selected_project):
                st.success(f"Loaded project: {selected_project}")
            else:
                st.error("Could not load project.")

    st.divider()

    st.subheader("Create New Project")
    new_project_name = st.text_input("Project name", key="new_project_name")
    new_topic = st.text_area("Research topic", key="new_topic", height=120)

    if st.button("Create Project", use_container_width=True):
        if not new_project_name.strip():
            st.error("Project name is required.")
        elif not new_topic.strip():
            st.error("Research topic is required.")
        else:
            record = upsert_project(
                project_name=new_project_name.strip(),
                topic=new_topic.strip(),
            )
            load_project_into_session(record["project_name"])
            st.success(f"Created project: {record['project_name']}")

    st.divider()

    st.subheader("Manage Current Project")

    rename_to = st.text_input("Rename current project to", key="rename_to")

    if st.button("Rename Project", use_container_width=True):
        if not ensure_active_project():
            st.stop()
        if not rename_to.strip():
            st.error("Enter a new name.")
        else:
            old_name = st.session_state.project_name
            if rename_project(old_name, rename_to.strip()):
                load_project_into_session(rename_to.strip())
                st.success(f"Renamed '{old_name}' to '{rename_to.strip()}'.")
            else:
                st.error("Rename failed. The new name may already exist.")

    if st.button("Delete Registry Record Only", use_container_width=True):
        if not ensure_active_project():
            st.stop()

        deleted = delete_project_record(st.session_state.project_name)
        if deleted:
            st.success("Deleted project record. Chroma memory was preserved.")
            st.session_state.project_name = ""
            st.session_state.project_id = ""
            st.session_state.topic = ""
            st.session_state.research_results = []
            st.session_state.visited_urls = []
            clear_workflow_outputs()
        else:
            st.error("Delete failed.")

    if st.button("Delete Project + Memory", use_container_width=True):
        if not ensure_active_project():
            st.stop()

        project_id = st.session_state.project_id
        deleted_count = reset_memory_for_project(project_id)
        deleted = delete_project_record(st.session_state.project_name)

        if deleted:
            st.success(f"Deleted project and {deleted_count} memory record(s).")
            st.session_state.project_name = ""
            st.session_state.project_id = ""
            st.session_state.topic = ""
            st.session_state.research_results = []
            st.session_state.visited_urls = []
            clear_workflow_outputs()
        else:
            st.error("Delete failed.")


# ======================================================
# CURRENT PROJECT HEADER
# ======================================================

col1, col2, col3 = st.columns([2, 3, 1])

with col1:
    st.subheader("Current Project")
    st.write(f"**Name:** {st.session_state.project_name or 'None'}")
    st.write(f"**ID:** {st.session_state.project_id or 'None'}")

with col2:
    st.write("**Topic:**")
    st.write(st.session_state.topic or "No project loaded.")

with col3:
    if st.session_state.project_id:
        memory_count = get_project_memory_count(st.session_state.project_id)
        st.metric("Memory Records", memory_count)


# ======================================================
# TABS
# ======================================================

tab1, tab2, tab3, tab4, tab5 = st.tabs(
    [
        "Run Research",
        "Project Memory",
        "Analysis",
        "Final Report",
        "Manage Projects",
    ]
)


# ======================================================
# TAB 1 - RUN RESEARCH
# ======================================================

with tab1:
    st.subheader("Run Research")

    if not st.session_state.project_id:
        st.info("Create or load a project to begin.")
    else:
        guidance = st.text_area(
            "Optional guidance for this research pass",
            value=st.session_state.human_guidance,
            height=100,
            help="Example: focus on pricing, compare competitors, find recent sources, identify risks.",
        )

        col_a, col_b = st.columns(2)

        with col_a:
            if st.button("Run One Research Pass", use_container_width=True):
                with st.spinner("Running research..."):
                    summary, sources, new_urls = web_research(
                        topic=st.session_state.topic,
                        human_guidance=guidance,
                        visited_urls=st.session_state.visited_urls,
                    )

                    if not summary or not new_urls:
                        st.warning("No new research results were found.")
                    else:
                        st.session_state.human_guidance = guidance
                        add_research_result(summary, sources, guidance)
                        clear_workflow_outputs()
                        st.success("Research pass completed and saved to memory.")

        with col_b:
            if st.button("Reload Memory From Storage", use_container_width=True):
                if load_project_into_session(st.session_state.project_name):
                    st.success("Reloaded project memory from Chroma.")
                else:
                    st.error("Failed to reload project.")

        st.markdown("### Latest Research Result")
        if st.session_state.research_results:
            latest = st.session_state.research_results[-1]
            st.markdown(latest["summary"])

            with st.expander("Latest sources"):
                for idx, source in enumerate(latest.get("sources", []), start=1):
                    st.write(f"{idx}. {format_source_label(source.get('title'), source.get('url'))}")
                    raw_content = source.get("raw_content", "")
                    if raw_content:
                        st.caption(truncate_text(raw_content, 250))
        else:
            st.write("No research entries yet.")


# ======================================================
# TAB 2 - PROJECT MEMORY
# ======================================================

with tab2:
    st.subheader("Project Memory")

    if not st.session_state.project_id:
        st.info("Load a project to inspect memory.")
    else:
        st.write(f"**Visited URLs:** {len(st.session_state.visited_urls)}")
        st.write(f"**Research Entries:** {len(st.session_state.research_results)}")

        if st.session_state.visited_urls:
            with st.expander("Visited URLs"):
                for url in st.session_state.visited_urls:
                    st.write(url)

        if st.session_state.research_results:
            for entry in st.session_state.research_results:
                with st.expander(f"Iteration {entry.get('iteration', '?')}"):
                    st.write(f"**Topic:** {entry.get('topic', '')}")
                    st.write(f"**Guidance:** {entry.get('human_feedback_context', '') or 'None'}")
                    st.markdown(entry.get("summary", ""))

                    st.write("**Sources:**")
                    for source in entry.get("sources", []):
                        st.write(f"- {format_source_label(source.get('title'), source.get('url'))}")
        else:
            st.write("No stored research yet.")


# ======================================================
# TAB 3 - ANALYSIS
# ======================================================

with tab3:
    st.subheader("Analysis")

    if not st.session_state.project_id:
        st.info("Load a project first.")
    elif not st.session_state.research_results:
        st.info("Run at least one research pass before analysis.")
    else:
        if st.button("Generate Analysis", use_container_width=True):
            with st.spinner("Generating analysis..."):
                analysis = generate_analysis(
                    research_results=st.session_state.research_results,
                    human_guidance=st.session_state.human_guidance,
                )
                st.session_state.analysis = analysis
                st.session_state.approved = is_analysis_approved(analysis)
                st.success("Analysis generated.")

        if st.session_state.analysis:
            st.markdown(st.session_state.analysis)
            st.write(f"**Model verdict:** {'Approved' if st.session_state.approved else 'Needs more research'}")

            refine_guidance = st.text_area(
                "Refinement guidance",
                value=st.session_state.human_guidance,
                key="refine_guidance",
                height=100,
            )

            col_x, col_y = st.columns(2)

            with col_x:
                if st.button("Approve Research", use_container_width=True):
                    st.session_state.approved = True
                    st.success("Research approved. You can now generate the final report.")

            with col_y:
                if st.button("Request Refinement", use_container_width=True):
                    st.session_state.approved = False
                    st.session_state.human_guidance = refine_guidance
                    st.info("Guidance saved. Run another research pass.")
        else:
            st.write("No analysis generated yet.")


# ======================================================
# TAB 4 - FINAL REPORT
# ======================================================

with tab4:
    st.subheader("Final Report")

    if not st.session_state.project_id:
        st.info("Load a project first.")
    elif not st.session_state.research_results:
        st.info("Run research before generating a report.")
    else:
        if st.button("Generate Final Report", use_container_width=True):
            with st.spinner("Generating final report..."):
                report = generate_final_report(
                    research_results=st.session_state.research_results,
                    human_guidance=st.session_state.human_guidance,
                )
                st.session_state.report = report
                st.success("Final report generated.")

        if st.session_state.report:
            st.markdown(st.session_state.report)

            st.download_button(
                label="Download Report as Markdown",
                data=st.session_state.report,
                file_name=f"{st.session_state.project_name or 'research_report'}.md",
                mime="text/markdown",
                use_container_width=True,
            )
        else:
            st.write("No final report generated yet.")


# ======================================================
# TAB 5 - MANAGE PROJECTS
# ======================================================

with tab5:
    st.subheader("Saved Projects")

    projects = list_projects()

    if not projects:
        st.write("No saved projects found.")
    else:
        rows = []
        for project in projects:
            rows.append(
                {
                    "Project Name": project.get("project_name", ""),
                    "Topic": project.get("topic", ""),
                    "Project ID": project.get("project_id", ""),
                    "Updated At": project.get("updated_at", ""),
                }
            )

        st.dataframe(rows, use_container_width=True)

        st.markdown("### Quick Load")
        quick_project = st.selectbox(
            "Select a project",
            options=[""] + [p["project_name"] for p in projects],
            key="quick_project",
        )

        if st.button("Load Selected Project", use_container_width=True):
            if quick_project and load_project_into_session(quick_project):
                st.success(f"Loaded project: {quick_project}")
            else:
                st.error("Could not load selected project.")