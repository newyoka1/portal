"""
Master Portal — entry point.
Run with: streamlit run app.py
"""
import streamlit as st

st.set_page_config(page_title="Politika Portal", page_icon="🏛️", layout="wide")

voter  = st.Page("pages/voter_pipeline.py",  title="NYS Voter Pipeline",    icon="🗳️")
fb_rec = st.Page("pages/fb_receipts.py",     title="FB Ads Receipts",       icon="🧾")
fb_app = st.Page("pages/fb_ad_approval.py",  title="FB Ad Approval",        icon="📋")

pg = st.navigation({
    "Voter & Research": [voter],
    "Facebook":         [fb_rec, fb_app],
})
pg.run()
