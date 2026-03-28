"""NYS Opposition Research — file browser, donor lookup, and Claude research assistant."""
import sys
from pathlib import Path

import streamlit as st

OPPO_DIR  = Path(r"D:\git\nys-opp-research")
RACES_DIR = OPPO_DIR / "races"

sys.path.insert(0, str(Path(r"D:\git\nys-voter-pipeline")))

st.title("🔍 NYS Opposition Research")

tab_races, tab_donors, tab_claude = st.tabs(["📁 Races & Files", "💰 Donor Lookup", "🤖 Research Assistant"])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — RACES & FILES
# ══════════════════════════════════════════════════════════════════════════════
with tab_races:
    races = sorted([d.name for d in RACES_DIR.iterdir() if d.is_dir()]) if RACES_DIR.exists() else []

    if not races:
        st.info("No race folders found under races/")
    else:
        selected_race = st.selectbox("Select race / candidate", races)
        race_dir      = RACES_DIR / selected_race

        st.subheader(selected_race)

        all_files = sorted(race_dir.iterdir(), key=lambda f: f.name)
        docs  = [f for f in all_files if f.suffix in (".docx", ".pdf")]
        data  = [f for f in all_files if f.suffix in (".sql", ".py", ".txt", ".md", ".csv")]

        if docs:
            st.markdown("**Documents**")
            for f in docs:
                with open(f, "rb") as fh:
                    mime = ("application/vnd.openxmlformats-officedocument"
                            ".wordprocessingml.document" if f.suffix == ".docx"
                            else "application/pdf")
                    st.download_button(
                        f"Download {f.name}", fh.read(), f.name,
                        mime=mime, key=f"dl_{f.name}",
                    )

        if data:
            st.markdown("**Research files**")
            for f in data:
                with st.expander(f.name):
                    try:
                        st.code(f.read_text(encoding="utf-8", errors="replace"),
                                language="sql" if f.suffix == ".sql"
                                else "python" if f.suffix == ".py"
                                else "markdown" if f.suffix == ".md"
                                else "text")
                    except Exception as e:
                        st.error(str(e))

        if not docs and not data:
            st.info("No files in this race folder yet.")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — DONOR LOOKUP
# ══════════════════════════════════════════════════════════════════════════════
with tab_donors:
    st.subheader("BOE Donor Lookup")
    st.caption("Searches the local boe_donors database (NYS state campaign finance).")

    col1, col2 = st.columns(2)
    search_type = col1.radio("Search by", ["Committee / Candidate", "Contributor name"],
                             horizontal=True)
    query_text  = col2.text_input("Search", placeholder="e.g. Mills, Eric Adams, NYSUT…")

    col3, col4 = st.columns(2)
    limit      = col3.number_input("Max rows", 50, 2000, 200, step=50)
    min_amount = col4.number_input("Min contribution ($)", 0, 10000, 0, step=100)

    if st.button("Search", type="primary") and query_text.strip():
        try:
            from utils.db import get_conn
            conn = get_conn("boe_donors", autocommit=True)
            cur  = conn.cursor()
            term = f"%{query_text.strip()}%"

            if "Committee" in search_type:
                sql = """
                    SELECT contribution_date, contributor_name, contributor_city,
                           contributor_state, amount, payment_type,
                           committee_name, candidate_name
                    FROM contributions
                    WHERE (committee_name LIKE %s OR candidate_name LIKE %s)
                      AND amount >= %s
                    ORDER BY contribution_date DESC LIMIT %s
                """
                cur.execute(sql, (term, term, min_amount, limit))
            else:
                sql = """
                    SELECT contribution_date, contributor_name, contributor_city,
                           contributor_state, amount, payment_type,
                           committee_name, candidate_name
                    FROM contributions
                    WHERE contributor_name LIKE %s AND amount >= %s
                    ORDER BY contribution_date DESC LIMIT %s
                """
                cur.execute(sql, (term, min_amount, limit))

            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            cur.close()
            conn.close()

            if not rows:
                st.info("No results found.")
            else:
                import pandas as pd
                df    = pd.DataFrame(rows, columns=cols)
                total = df["amount"].sum()
                st.caption(f"{len(df)} rows · Total: ${total:,.2f}")
                st.dataframe(df, use_container_width=True, hide_index=True)
                st.download_button("Download CSV",
                                   df.to_csv(index=False).encode(),
                                   f"donors_{query_text[:30].replace(' ','_')}.csv",
                                   mime="text/csv")

        except Exception as e:
            st.error(f"Database error: {e}")
            st.caption("Make sure the local MySQL server is running and boe_donors is loaded.")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — CLAUDE RESEARCH ASSISTANT
# ══════════════════════════════════════════════════════════════════════════════
with tab_claude:
    import ollama as _ollama

    MODEL = "llama3.1"

    SYSTEM_PROMPT = """\
You are an expert NYS political opposition research analyst. You help build research
dossiers on candidates for New York State legislative and local races.

## Standard Report Structure
Every research report follows 11 sections:
I.   Biographical Profile (snapshot table + career timeline)
II.  Credential & Title Claims — Key Vulnerabilities (inflated credentials)
III. Corporate Background & Client Conflicts (PR/lobbying/corporate employers)
IV.  Electoral History (prior races, results, loss record)
V.   City/Local Council Record (legislative votes, accomplishments)
VI.  Roots vs. Reality (residency, carpetbagger analysis)
VII. Campaign Finance & Financial Record (BOE donor breakdown, geographic analysis)
VIII.Book/Publication Analysis (self-published works, contradictions)
IX.  Suggested Attack Lines & Messaging (polished lines by theme)
X.   Open Research Items (status table with HOW TO OBTAIN column)
XI.  Sources & References (organized by category)

Every report opens with an Executive Summary Vulnerability Table (top 5-7 attack vectors).

## Data Sources Available Locally
- boe_donors.contributions — ~4.28M NYS state campaign finance rows
- nys_voter_tagging.voter_file — 13M+ NYS voter records with donation totals

## Your Role
- Help initiate and structure new candidate research
- Draft sections of the research report
- Suggest attack lines, vulnerabilities, and open research items
- Formulate SQL queries to run against the local databases
- Identify what public records to search (NYSED, Ballotpedia, local news, etc.)
"""

    # ── Check Ollama is reachable ──────────────────────────────────────────
    try:
        _ollama.list()
        ollama_ok = True
    except Exception:
        ollama_ok = False

    if not ollama_ok:
        st.warning("Ollama is not running. Start it from the Start menu or taskbar.")
        st.stop()

    # ── Sidebar ────────────────────────────────────────────────────────────
    with st.sidebar:
        st.subheader("Research Session")
        races_list = (sorted([d.name for d in RACES_DIR.iterdir() if d.is_dir()])
                      if RACES_DIR.exists() else [])
        context_race = st.selectbox("Focus race (optional)", ["— none —"] + races_list,
                                    key="ollama_race_select")
        if st.button("Clear conversation", key="clear_oppo"):
            st.session_state.oppo_messages = []
            st.rerun()

    # ── Session state ──────────────────────────────────────────────────────
    if "oppo_messages" not in st.session_state:
        st.session_state.oppo_messages = []

    # ── Race context ───────────────────────────────────────────────────────
    race_context = ""
    if context_race and context_race != "— none —":
        research_md = RACES_DIR / context_race / "research.md"
        if research_md.exists():
            race_context = (f"\n\n## Existing research notes for {context_race}:\n"
                            + research_md.read_text(encoding="utf-8", errors="replace")[:6000])

    st.caption(f"Model: {MODEL} · running locally via Ollama (no API cost)")

    # ── Render chat history ────────────────────────────────────────────────
    for msg in st.session_state.oppo_messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    # ── Chat input ─────────────────────────────────────────────────────────
    if prompt := st.chat_input("Start new research, ask about a candidate, request SQL queries…"):
        st.session_state.oppo_messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        def _stream():
            stream = _ollama.chat(
                model=MODEL,
                messages=[{"role": "system", "content": SYSTEM_PROMPT + race_context}]
                         + st.session_state.oppo_messages,
                stream=True,
            )
            for chunk in stream:
                yield chunk["message"]["content"]

        with st.chat_message("assistant"):
            response_text = st.write_stream(_stream())

        st.session_state.oppo_messages.append(
            {"role": "assistant", "content": response_text}
        )
