"""Email Approval — embeds the Railway-hosted app."""
import os
import streamlit as st
import streamlit.components.v1 as components

URL = os.getenv("EMAIL_APPROVAL_URL", "http://localhost:8000")

st.title("📧 Email Approval")

col_caption, col_btn = st.columns([3, 1])
col_caption.caption(f"Hosted on Railway · {URL}")
col_btn.link_button("Open in new tab", URL, use_container_width=True)

st.divider()

components.iframe(URL, height=840, scrolling=True)
