import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "shared"))

import streamlit as st
import duckdb
import json
from bedrock_helper import call_nova_lite, call_nova_pro

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "shared", "sigma_platform.duckdb")

# ── Page config ───────────────────────────────────────────────
st.set_page_config(
    page_title="CFO Challenger — Sigma DataTech",
    page_icon="💼",
    layout="wide",
)

# ── Custom CSS ────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
}

/* Header gradient */
.main-header {
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
    padding: 2rem 2.5rem;
    border-radius: 16px;
    margin-bottom: 2rem;
    border: 1px solid rgba(255,255,255,0.08);
}
.main-header h1 {
    color: #e2e8f0;
    font-size: 2rem;
    font-weight: 700;
    margin: 0;
}
.main-header p {
    color: #94a3b8;
    margin: 0.4rem 0 0 0;
    font-size: 0.95rem;
}

/* Round cards */
.round-card {
    background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
    border-radius: 14px;
    padding: 1.5rem 1.8rem;
    margin-bottom: 1.5rem;
    border: 1px solid rgba(255,255,255,0.06);
    box-shadow: 0 4px 24px rgba(0,0,0,0.3);
}
.round-label {
    font-size: 0.75rem;
    font-weight: 600;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    margin-bottom: 0.4rem;
}
.round1-label { color: #60a5fa; }
.round2-label { color: #f59e0b; }
.round3-label { color: #34d399; }

/* Verdict badges */
.verdict-verified {
    background: linear-gradient(135deg, #065f46, #047857);
    color: #a7f3d0;
    padding: 0.35rem 1rem;
    border-radius: 999px;
    font-size: 0.8rem;
    font-weight: 700;
    display: inline-block;
    margin: 0.3rem 0;
    border: 1px solid #10b981;
    letter-spacing: 0.05em;
}
.verdict-wrong {
    background: linear-gradient(135deg, #7f1d1d, #991b1b);
    color: #fca5a5;
    padding: 0.35rem 1rem;
    border-radius: 999px;
    font-size: 0.8rem;
    font-weight: 700;
    display: inline-block;
    margin: 0.3rem 0;
    border: 1px solid #ef4444;
    letter-spacing: 0.05em;
}
.verdict-misleading {
    background: linear-gradient(135deg, #78350f, #92400e);
    color: #fcd34d;
    padding: 0.35rem 1rem;
    border-radius: 999px;
    font-size: 0.8rem;
    font-weight: 700;
    display: inline-block;
    margin: 0.3rem 0;
    border: 1px solid #f59e0b;
    letter-spacing: 0.05em;
}

/* Trust score */
.trust-score-card {
    background: linear-gradient(135deg, #0f172a, #1e293b);
    border: 1px solid rgba(99,102,241,0.4);
    border-radius: 14px;
    padding: 1.8rem;
    text-align: center;
}
.trust-score-number {
    font-size: 4rem;
    font-weight: 800;
    background: linear-gradient(135deg, #818cf8, #60a5fa);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
}

/* SQL box */
.sql-block {
    background: #0a0f1e;
    border: 1px solid rgba(96,165,250,0.2);
    border-radius: 8px;
    padding: 1rem;
    font-family: 'Courier New', monospace;
    font-size: 0.82rem;
    color: #93c5fd;
    overflow-x: auto;
    margin: 0.5rem 0;
}

/* Data table */
.data-result {
    background: #020817;
    border: 1px solid rgba(52,211,153,0.2);
    border-radius: 8px;
    padding: 0.8rem 1rem;
    font-family: 'Courier New', monospace;
    font-size: 0.82rem;
    color: #6ee7b7;
    margin: 0.5rem 0;
}

/* Metric boxes */
.metric-row {
    display: flex;
    gap: 1rem;
    margin: 1rem 0;
}
.metric-box {
    flex: 1;
    background: linear-gradient(135deg, #1e293b, #0f172a);
    border-radius: 10px;
    padding: 1rem 1.2rem;
    border: 1px solid rgba(255,255,255,0.07);
    text-align: center;
}
.metric-value {
    font-size: 1.6rem;
    font-weight: 700;
    color: #e2e8f0;
}
.metric-label {
    font-size: 0.75rem;
    color: #64748b;
    margin-top: 0.2rem;
}

/* Step number badges */
.step-badge {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 28px;
    height: 28px;
    border-radius: 50%;
    font-size: 0.8rem;
    font-weight: 700;
    margin-right: 0.5rem;
}
.step-badge-1 { background: rgba(96,165,250,0.2); color: #60a5fa; border: 1px solid #60a5fa; }
.step-badge-2 { background: rgba(245,158,11,0.2); color: #f59e0b; border: 1px solid #f59e0b; }
.step-badge-3 { background: rgba(52,211,153,0.2); color: #34d399; border: 1px solid #34d399; }

/* Divider */
.sigma-divider {
    border: none;
    border-top: 1px solid rgba(255,255,255,0.06);
    margin: 1.5rem 0;
}

/* Warning slide */
.wrong-slide {
    background: linear-gradient(135deg, #1c0a0a, #2d1010);
    border: 1px solid rgba(239,68,68,0.4);
    border-radius: 14px;
    padding: 1.5rem 1.8rem;
    margin-top: 1.5rem;
}
</style>
""", unsafe_allow_html=True)

# ── Header ─────────────────────────────────────────────────────
st.markdown("""
<div class="main-header">
    <h1>💼 CFO Challenger</h1>
    <p>Sigma DataTech AI Ops Platform — Day 9 · Team 4 &nbsp;|&nbsp; CEO Briefing Intelligence System</p>
</div>
""", unsafe_allow_html=True)

# ── DB Connection ──────────────────────────────────────────────
@st.cache_resource
def get_conn():
    return duckdb.connect(DB_PATH, read_only=True)

conn = get_conn()

# ── Fetch Gold Metrics ─────────────────────────────────────────
@st.cache_data
def fetch_gold_metrics():
    daily = conn.execute("""
        SELECT report_date, total_revenue, total_txns, unique_customers,
               unique_merchants, failure_rate_pct
        FROM gold_daily_summary
        ORDER BY report_date
    """).fetchall()

    merchants = conn.execute("""
        SELECT merchant_name, category, city, total_revenue, txn_count, failure_rate_pct
        FROM gold_merchant_performance
        ORDER BY total_revenue DESC
    """).fetchall()

    summary = conn.execute("""
        SELECT
            SUM(total_revenue) as total_rev,
            SUM(total_txns) as total_txns,
            AVG(failure_rate_pct) as avg_fail_pct,
            MIN(total_revenue) as min_day_rev,
            MAX(total_revenue) as max_day_rev
        FROM gold_daily_summary
    """).fetchone()

    category_rev = conn.execute("""
        SELECT category, SUM(total_revenue) as rev, SUM(txn_count) as txns
        FROM gold_merchant_performance
        GROUP BY category
        ORDER BY rev DESC
    """).fetchall()

    return {
        "daily": daily,
        "merchants": merchants,
        "summary": summary,
        "category_rev": category_rev
    }

metrics = fetch_gold_metrics()

# ── Build context string for AI ────────────────────────────────
def build_context(metrics):
    daily_lines = "\n".join(
        f"  {r[0]}: Revenue=₹{r[1]:,.1f}, Txns={r[2]}, Customers={r[3]}, FailureRate={r[5]:.1f}%"
        for r in metrics["daily"]
    )
    merch_lines = "\n".join(
        f"  {r[0]} ({r[1]}, {r[2]}): Revenue=₹{r[3]:,.1f}, Txns={r[4]}, FailRate={r[5]:.1f}%"
        for r in metrics["merchants"]
    )
    cat_lines = "\n".join(
        f"  {r[0]}: Revenue=₹{r[1]:,.1f}, Txns={r[2]}"
        for r in metrics["category_rev"]
    )
    s = metrics["summary"]
    return f"""
GOLD LAYER METRICS — Sigma DataTech Platform (Jan 2024)

DAILY SUMMARY ({len(metrics['daily'])} days of data):
{daily_lines}

AGGREGATE:
  Total Revenue: ₹{s[0]:,.1f}
  Total Transactions: {s[1]}
  Average Failure Rate: {s[2]:.1f}%
  Lowest Revenue Day: ₹{s[3]:,.1f}
  Highest Revenue Day: ₹{s[4]:,.1f}

MERCHANT PERFORMANCE:
{merch_lines}

REVENUE BY CATEGORY:
{cat_lines}
"""

context = build_context(metrics)

# ── Session State Init ─────────────────────────────────────────
if "briefing" not in st.session_state:
    st.session_state.briefing = None
if "challenges" not in st.session_state:
    st.session_state.challenges = None
if "factchecks" not in st.session_state:
    st.session_state.factchecks = None

# ══════════════════════════════════════════════════════════════
# ROUND 1 — AI BRIEFING WRITER
# ══════════════════════════════════════════════════════════════
st.markdown("""
<div class="round-label round1-label">
    <span class="step-badge step-badge-1">1</span> Round 1 — AI Briefing Writer
</div>
""", unsafe_allow_html=True)

col_r1a, col_r1b = st.columns([3, 1])
with col_r1a:
    st.markdown("**Nova Pro** generates a 5-bullet executive briefing from Gold layer data.")
with col_r1b:
    gen_briefing = st.button("📝 Generate CEO Briefing", use_container_width=True, type="primary")

if gen_briefing or st.session_state.briefing:
    if gen_briefing:
        with st.spinner("Nova Pro is writing the CEO briefing..."):
            system_prompt = """You are a senior data analyst at Sigma DataTech writing the Monday morning CEO revenue briefing.
You must write EXACTLY 5 bullet points. Each bullet must contain:
- A specific number from the data
- A trend or comparison
- A business insight or recommendation

Be confident and authoritative. Include at least one insight about revenue trends over time,
one about top merchants, one about failure rates, one about category performance,
and one forward-looking recommendation.

IMPORTANT: One of your insights should sound compelling but be based on very few data points
(e.g., drawing a strong trend conclusion from only 1-2 data points).

Format your response as JSON with this exact structure:
{
  "bullets": [
    {"id": 1, "title": "short title", "claim": "full bullet text with specific numbers"},
    {"id": 2, "title": "short title", "claim": "full bullet text with specific numbers"},
    {"id": 3, "title": "short title", "claim": "full bullet text with specific numbers"},
    {"id": 4, "title": "short title", "claim": "full bullet text with specific numbers"},
    {"id": 5, "title": "short title", "claim": "full bullet text with specific numbers"}
  ]
}"""
            user_prompt = f"""Generate a 5-bullet CEO revenue briefing based on this Gold layer data:

{context}

Return ONLY valid JSON, no markdown, no explanation."""

            raw = call_nova_pro(system_prompt, user_prompt, max_tokens=1500)
            # Strip markdown fences if present
            raw = raw.strip()
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            raw = raw.strip()
            try:
                st.session_state.briefing = json.loads(raw)
            except Exception:
                # Fallback: wrap in structure
                st.session_state.briefing = {"bullets": [{"id": 1, "title": "Briefing", "claim": raw}]}
            st.session_state.challenges = None
            st.session_state.factchecks = None

    if st.session_state.briefing:
        st.markdown('<div class="round-card">', unsafe_allow_html=True)
        st.markdown("#### 📋 CEO Revenue Briefing — Sigma DataTech (Jan 2024)")
        bullets = st.session_state.briefing.get("bullets", [])
        icons = ["📈", "🏆", "⚠️", "🗂️", "🔭"]
        for i, b in enumerate(bullets):
            icon = icons[i % len(icons)]
            st.markdown(f"""
**{icon} {b.get('title', f'Insight {i+1}')}**

{b.get('claim', '')}
""")
        st.markdown('</div>', unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════
# ROUND 2 — CFO CHALLENGE
# ══════════════════════════════════════════════════════════════
st.markdown('<hr class="sigma-divider">', unsafe_allow_html=True)
st.markdown("""
<div class="round-label round2-label">
    <span class="step-badge step-badge-2">2</span> Round 2 — CFO Challenge
</div>
""", unsafe_allow_html=True)

col_r2a, col_r2b = st.columns([3, 1])
with col_r2a:
    st.markdown("**Nova Lite** plays a skeptical CFO and challenges 3 claims from the briefing.")
with col_r2b:
    gen_challenge = st.button(
        "🔥 CFO Challenge",
        use_container_width=True,
        disabled=st.session_state.briefing is None,
        type="primary" if st.session_state.briefing else "secondary"
    )

if gen_challenge or st.session_state.challenges:
    if gen_challenge and st.session_state.briefing:
        with st.spinner("CFO is reviewing the briefing..."):
            bullets_text = "\n".join(
                f"Bullet {b['id']}: {b.get('title','')}: {b.get('claim','')}"
                for b in st.session_state.briefing.get("bullets", [])
            )
            system_prompt = """You are a sharp, skeptical CFO at Sigma DataTech.
You have just read the CEO briefing and you are NOT impressed. You will challenge 3 specific claims.

For each challenge:
1. Quote the EXACT claim being challenged
2. State WHY you're skeptical (statistical weakness, sample size, misleading comparison, etc.)
3. Ask "Show me the data" with a SPECIFIC question that requires running a query

Be aggressive but professional. Focus especially on:
- Claims based on too few data points
- Trends drawn from insufficient samples
- Percentages that sound impressive but come from tiny absolutes
- Averages that hide important variance

Return ONLY valid JSON with this structure:
{
  "challenges": [
    {
      "id": 1,
      "bullet_ref": <bullet id being challenged>,
      "claim_quoted": "exact quote from the briefing",
      "cfo_objection": "why I'm skeptical",
      "data_demand": "Show me the data: specific question to answer"
    },
    ...3 challenges total...
  ]
}"""
            user_prompt = f"""Here is the CEO briefing you must challenge:

{bullets_text}

Pick the 3 weakest claims and challenge them as the CFO. Return ONLY valid JSON."""

            raw = call_nova_lite(system_prompt, user_prompt, max_tokens=1200)
            raw = raw.strip()
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            raw = raw.strip()
            try:
                st.session_state.challenges = json.loads(raw)
            except Exception:
                st.session_state.challenges = {"challenges": [{"id": 1, "bullet_ref": 1,
                    "claim_quoted": "N/A", "cfo_objection": raw, "data_demand": ""}]}
            st.session_state.factchecks = None

    if st.session_state.challenges:
        challenges = st.session_state.challenges.get("challenges", [])
        cfo_icons = ["❓", "🧐", "⚡"]
        for i, c in enumerate(challenges):
            with st.expander(f"CFO Challenge #{c.get('id', i+1)} — Bullet {c.get('bullet_ref', '?')}", expanded=True):
                st.markdown(f"""
<div style="background:rgba(245,158,11,0.08); border-left: 3px solid #f59e0b; padding: 0.8rem 1rem; border-radius: 4px; margin-bottom: 0.8rem;">
<em>"{c.get('claim_quoted', '')}"</em>
</div>

**{cfo_icons[i % 3]} CFO Objection:** {c.get('cfo_objection', '')}

**📊 Data Demand:** _{c.get('data_demand', '')}_
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════
# ROUND 3 — FACT CHECK
# ══════════════════════════════════════════════════════════════
st.markdown('<hr class="sigma-divider">', unsafe_allow_html=True)
st.markdown("""
<div class="round-label round3-label">
    <span class="step-badge step-badge-3">3</span> Round 3 — Fact Check
</div>
""", unsafe_allow_html=True)

col_r3a, col_r3b = st.columns([3, 1])
with col_r3a:
    st.markdown("**DuckDB queries** verify or refute each CFO challenge. Verdicts: VERIFIED / WRONG / MISLEADING")
with col_r3b:
    run_factcheck = st.button(
        "🔍 Run Fact Check",
        use_container_width=True,
        disabled=st.session_state.challenges is None,
        type="primary" if st.session_state.challenges else "secondary"
    )

# Pre-defined fact-check queries targeting known data patterns
FACTCHECK_QUERIES = {
    "revenue_trend": {
        "sql": """SELECT report_date, total_revenue, total_txns
FROM gold_daily_summary
ORDER BY report_date""",
        "description": "Daily revenue trend with transaction counts"
    },
    "top_merchants": {
        "sql": """SELECT merchant_name, total_revenue, txn_count, failure_rate_pct
FROM gold_merchant_performance
ORDER BY total_revenue DESC""",
        "description": "All merchants ranked by revenue"
    },
    "failure_rate": {
        "sql": """SELECT merchant_name, txn_count, failure_rate_pct,
       ROUND(txn_count * failure_rate_pct / 100) as failed_txns
FROM gold_merchant_performance
WHERE failure_rate_pct > 0
ORDER BY failure_rate_pct DESC""",
        "description": "Merchants with failures — absolute counts vs percentages"
    },
    "category_revenue": {
        "sql": """SELECT category, SUM(total_revenue) as total_rev,
       SUM(txn_count) as total_txns,
       ROUND(AVG(total_revenue), 2) as avg_rev_per_merchant
FROM gold_merchant_performance
GROUP BY category
ORDER BY total_rev DESC""",
        "description": "Revenue by category"
    },
    "sample_size_check": {
        "sql": """SELECT report_date, total_revenue, total_txns,
       CASE WHEN total_txns <= 1 THEN 'SINGLE TRANSACTION — UNRELIABLE'
            WHEN total_txns <= 2 THEN 'TINY SAMPLE — SUSPICIOUS'
            ELSE 'ACCEPTABLE'
       END as sample_warning
FROM gold_daily_summary
ORDER BY report_date""",
        "description": "Sample size reliability check for each day"
    },
    "flipkart_anomaly": {
        "sql": """SELECT gs.report_date, gs.total_revenue, gs.total_txns,
       gm.merchant_name, gm.total_revenue as merchant_total
FROM gold_daily_summary gs
JOIN silver_transactions st ON gs.report_date = st.transaction_date
JOIN gold_merchant_performance gm ON st.merchant_id = gm.merchant_id
WHERE gs.report_date IN ('2024-01-25')
ORDER BY gs.report_date""",
        "description": "Jan 25 peak revenue — driven by single Flipkart transaction"
    }
}

if run_factcheck or st.session_state.factchecks:
    if run_factcheck and st.session_state.challenges:
        with st.spinner("Running DuckDB queries and generating verdicts..."):
            challenges = st.session_state.challenges.get("challenges", [])

            # Run all standard queries
            query_results = {}
            for key, q in FACTCHECK_QUERIES.items():
                try:
                    rows = conn.execute(q["sql"]).fetchall()
                    cols = [d[0] for d in conn.execute(q["sql"]).description]
                    query_results[key] = {"rows": rows, "cols": cols, "sql": q["sql"], "desc": q["description"]}
                except Exception as e:
                    query_results[key] = {"error": str(e), "sql": q["sql"]}

            # Ask Nova Lite to generate verdict for each challenge
            factchecks = []
            for c in challenges:
                qr_text = "\n\n".join(
                    f"Query: {v['sql']}\nResult:\n" + "\n".join(str(r) for r in v.get("rows", [])[:8])
                    for v in query_results.values() if "rows" in v
                )
                system_v = """You are a data auditor. Given a CFO's challenge and actual DuckDB query results,
determine the verdict. Return ONLY valid JSON:
{
  "verdict": "VERIFIED" | "WRONG" | "MISLEADING",
  "explanation": "2-3 sentence explanation citing specific numbers from the data",
  "key_query": "the most relevant SQL query that proves your verdict",
  "key_result": "the specific data row or number that settles the question",
  "why_misleading": "if MISLEADING, explain the statistical flaw in detail (sample size, etc.)"
}"""
                user_v = f"""CFO Challenge:
Claim: {c.get('claim_quoted', '')}
Objection: {c.get('cfo_objection', '')}
Data Demand: {c.get('data_demand', '')}

Actual DuckDB Results:
{qr_text}

Verdict?"""
                try:
                    raw_v = call_nova_lite(system_v, user_v, max_tokens=600)
                    raw_v = raw_v.strip()
                    if raw_v.startswith("```"):
                        raw_v = raw_v.split("```")[1]
                        if raw_v.startswith("json"):
                            raw_v = raw_v[4:]
                    raw_v = raw_v.strip()
                    verdict_data = json.loads(raw_v)
                except Exception:
                    verdict_data = {
                        "verdict": "MISLEADING",
                        "explanation": raw_v if "raw_v" in dir() else "Parse error",
                        "key_query": "",
                        "key_result": "",
                        "why_misleading": ""
                    }
                factchecks.append({
                    "challenge": c,
                    "verdict": verdict_data,
                    "query_results": query_results
                })

            st.session_state.factchecks = factchecks

    if st.session_state.factchecks:
        factchecks = st.session_state.factchecks

        verdict_map = {
            "VERIFIED": ("verdict-verified", "✅ VERIFIED"),
            "WRONG": ("verdict-wrong", "❌ WRONG"),
            "MISLEADING": ("verdict-misleading", "⚠️ MISLEADING"),
        }

        for fc in factchecks:
            c = fc["challenge"]
            v = fc["verdict"]
            verdict_key = v.get("verdict", "MISLEADING").upper()
            css_class, verdict_label = verdict_map.get(verdict_key, ("verdict-misleading", "⚠️ MISLEADING"))

            with st.expander(f"Fact Check — CFO Challenge #{c.get('id', '?')}", expanded=True):
                col_v1, col_v2 = st.columns([2, 1])
                with col_v1:
                    st.markdown(f"**Claim:** _{c.get('claim_quoted', '')}_")
                with col_v2:
                    st.markdown(f'<span class="{css_class}">{verdict_label}</span>', unsafe_allow_html=True)

                st.markdown("**📝 Explanation:**")
                st.info(v.get("explanation", ""))

                if v.get("key_query"):
                    st.markdown("**🔍 Key Query:**")
                    st.code(v.get("key_query", ""), language="sql")

                if v.get("key_result"):
                    st.markdown("**📊 Result:**")
                    st.markdown(f'<div class="data-result">{v.get("key_result", "")}</div>', unsafe_allow_html=True)

                if verdict_key == "MISLEADING" and v.get("why_misleading"):
                    st.markdown("**⚠️ Why It's Statistically Misleading:**")
                    st.warning(v.get("why_misleading", ""))

        # ── Sample size diagnostic table ──────────────────────────
        st.markdown('<hr class="sigma-divider">', unsafe_allow_html=True)
        st.markdown("#### 📊 DuckDB Evidence: All Fact-Check Queries")

        tab_names = ["Daily Trend", "Merchant Ranking", "Failure Rates", "Category Revenue", "Sample Size Check", "Jan 25 Anomaly"]
        tabs = st.tabs(tab_names)
        query_keys = list(FACTCHECK_QUERIES.keys())

        qr = st.session_state.factchecks[0]["query_results"] if st.session_state.factchecks else {}
        for tab, key in zip(tabs, query_keys):
            with tab:
                q_data = qr.get(key, {})
                st.code(q_data.get("sql", ""), language="sql")
                if "rows" in q_data:
                    st.dataframe(
                        {col: [r[i] for r in q_data["rows"]] for i, col in enumerate(q_data.get("cols", []))},
                        use_container_width=True
                    )
                elif "error" in q_data:
                    st.error(q_data["error"])

# ══════════════════════════════════════════════════════════════
# WHAT AI GOT WRONG — THE TRAP
# ══════════════════════════════════════════════════════════════
st.markdown('<hr class="sigma-divider">', unsafe_allow_html=True)
st.markdown('<div class="round-label" style="color:#f87171;"><span class="step-badge" style="background:rgba(239,68,68,0.2);color:#f87171;border:1px solid #f87171;">!</span> The Trap — What AI Got Wrong</div>', unsafe_allow_html=True)

st.markdown("""
<div class="wrong-slide">
<h4 style="color:#f87171; margin-top:0;">🎯 The Statistically Invalid Insight</h4>
""", unsafe_allow_html=True)

trap_col1, trap_col2 = st.columns(2)
with trap_col1:
    st.markdown("""
**The Claim:**
> *"Revenue peaked on January 25th with ₹3,400 — a 335% surge — indicating strong late-month momentum and predicting continued growth."*

**Why It Sounds Convincing:**
- The number is real (₹3,400 on Jan 25 ✅)
- The percentage is mathematically correct (vs. ₹781 avg on other days ✅)
- "Late-month momentum" sounds like pattern analysis ✅

**Why It's Statistically Invalid:**
- Jan 25 had **exactly 1 transaction** (Flipkart, ₹3,400)
- You cannot draw a "trend" or "momentum" from n=1
- Remove that single transaction → the "surge" vanishes entirely
- Predicting "continued growth" from 1 data point is textbook **overfitting**
""")

with trap_col2:
    # Show the sample-size data
    rows = conn.execute("""
        SELECT report_date, total_revenue, total_txns,
               ROUND(total_revenue / total_txns, 2) as avg_txn_value,
               CASE WHEN total_txns = 1 THEN '🚨 n=1 — UNRELIABLE'
                    WHEN total_txns = 2 THEN '⚠️ n=2 — WEAK'
                    ELSE '✅ OK'
               END as reliability
        FROM gold_daily_summary
        ORDER BY report_date
    """).fetchall()
    cols = ["Date", "Revenue (₹)", "Txns", "Avg Txn (₹)", "Reliability"]
    data = {c: [r[i] for r in rows] for i, c in enumerate(cols)}
    st.dataframe(data, use_container_width=True)

st.markdown("""
**What Additional Data Would Make the Insight Valid?**
- At least **30 days** of consistent transaction data (Central Limit Theorem baseline)
- Minimum **10+ transactions per day** to compute meaningful daily averages
- Historical comparison: same date range in prior months/years
- Segmentation: is the Flipkart spike seasonal, promotional, or anomalous?

</div>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════
# TRUST SCORE
# ══════════════════════════════════════════════════════════════
st.markdown('<hr class="sigma-divider">', unsafe_allow_html=True)
st.markdown("#### 🎯 AI Trust Score for Business Insights")

ts_col1, ts_col2, ts_col3 = st.columns([1, 2, 1])

with ts_col1:
    st.markdown("""
<div class="trust-score-card">
    <div style="color:#94a3b8; font-size:0.8rem; font-weight:600; letter-spacing:0.1em; text-transform:uppercase; margin-bottom:0.5rem;">Trust Score</div>
    <div class="trust-score-number">48%</div>
    <div style="color:#64748b; font-size:0.8rem; margin-top:0.3rem;">Conditionally Trustworthy</div>
</div>
""", unsafe_allow_html=True)

with ts_col2:
    st.markdown("""
| Dimension | Score | Reasoning |
|-----------|-------|-----------|
| **Factual Accuracy** | 70% | Numbers are correct but framing is selective |
| **Statistical Validity** | 30% | Trends drawn from n=1 to n=2 samples are meaningless |
| **Sample Representativeness** | 40% | 9 days, mostly 1-2 txns/day — far too sparse |
| **Trend Reliability** | 35% | Cannot reliably establish trends with <30 data points |
| **Merchant Insights** | 65% | Merchant rankings are accurate but failure rates mislead |
| **Overall Trust** | **48%** | **Use as hypothesis generator, not decision basis** |

""")

with ts_col3:
    st.markdown("""
<div style="background: linear-gradient(135deg, #0f172a, #1e293b); border-radius: 10px; padding: 1rem; border: 1px solid rgba(255,255,255,0.07);">
<div style="color:#94a3b8; font-size:0.75rem; font-weight:600; text-transform:uppercase; margin-bottom:0.8rem;">Verdict</div>
<div style="color:#e2e8f0; font-size:0.85rem; line-height:1.6;">
AI insights are <strong style="color:#fbbf24;">useful as hypotheses</strong> but require human validation before any business decision.
<br><br>
<strong style="color:#f87171;">Never</strong> present AI-generated trends to the board without running the DuckDB queries first.
</div>
</div>
""", unsafe_allow_html=True)

# ── Footer ────────────────────────────────────────────────────
st.markdown('<hr class="sigma-divider">', unsafe_allow_html=True)
st.markdown("""
<div style="text-align:center; color:#334155; font-size:0.78rem; padding:0.5rem 0 1.5rem 0;">
Sigma DataTech AI Ops Platform · Day 9 · Team 4 — CFO Challenger · Built with Nova Pro + Nova Lite + DuckDB
</div>
""", unsafe_allow_html=True)
