from datetime import datetime
from html import escape
from zoneinfo import ZoneInfo


REGIME_ZH_MAP = {
    "Risk-on": "风险偏好回升",
    "Risk-off": "风险偏好回落",
    "Mixed": "分化 / 混合",
}

MODULE_ZH_MAP = {
    "Macro/Geopolitics": "宏观 / 地缘",
    "Commodities": "大宗商品",
    "Rates": "利率 / 国债",
    "Equities": "板块 / 股票",
    "FX": "外汇",
}


def format_number(value, digits=2):
    if value is None:
        return "n/a"
    return f"{value:.{digits}f}"


def format_signed(value, digits=2, suffix=""):
    if value is None:
        return "n/a"
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.{digits}f}{suffix}"


def change_color(value):
    if value is None:
        return "#64748b"
    if value > 0:
        return "#b45309"
    if value < 0:
        return "#0f766e"
    return "#64748b"


def regime_to_zh(regime):
    return REGIME_ZH_MAP.get(regime, regime)


def module_to_zh(module):
    return MODULE_ZH_MAP.get(module, module)


def safe_text(value, default=""):
    text = str(value or "").strip()
    if text:
        return text
    return str(default or "").strip()


def safe_escape(value, default=""):
    return escape(safe_text(value, default))


def build_market_rows(items, percent_suffix=True):
    rows = []
    for item in items:
        price = format_number(item.get("price"))
        unit = safe_text(item.get("unit"))
        if price != "n/a" and unit:
            price = f"{price} {unit}"
        change = format_signed(item.get("change"))
        change_pct = format_signed(item.get("change_pct"), suffix="%" if percent_suffix else "")
        source = item.get("source") or ""
        stale = bool(item.get("stale"))
        source_suffix = ""
        if source:
            source_suffix = f" | {source}"
        if stale:
            source_suffix += " | cached"

        rows.append(
            f"""
            <tr>
              <td style="padding:10px 12px;border-bottom:1px solid #e2e8f0;font-weight:700;color:#0f172a;">
                <div>{safe_escape(item.get('label'), 'Unknown')}</div>
                <div style="font-size:11px;color:#94a3b8;font-weight:600;">{safe_escape(item.get('symbol'))}{safe_escape(source_suffix)}</div>
              </td>
              <td style="padding:10px 12px;border-bottom:1px solid #e2e8f0;color:#1e293b;">{safe_escape(price)}</td>
              <td style="padding:10px 12px;border-bottom:1px solid #e2e8f0;color:{change_color(item.get('change'))};">{safe_escape(change)}</td>
              <td style="padding:10px 12px;border-bottom:1px solid #e2e8f0;color:{change_color(item.get('change_pct'))};">{safe_escape(change_pct)}</td>
            </tr>
            """
        )
    if not rows:
        rows.append(
            """
            <tr>
              <td colspan="4" style="padding:10px 12px;color:#64748b;">No market data available.</td>
            </tr>
            """
        )
    return "".join(rows)


def build_rates_card(rates):
    series = rates.get("series", {}) if isinstance(rates, dict) else {}
    us_2y = series.get("us_2y", {})
    us_10y = series.get("us_10y", {})
    curve = rates.get("curve_10y_2y_bps")
    us_2y_value = format_number(us_2y.get("value"))
    us_10y_value = format_number(us_10y.get("value"))
    us_2y_display = f"{us_2y_value}%" if us_2y_value != "n/a" else us_2y_value
    us_10y_display = f"{us_10y_value}%" if us_10y_value != "n/a" else us_10y_value
    source = rates.get("source") or "unknown"
    cached_at = rates.get("cached_at")
    footer = f"Source: {source}"
    if cached_at:
        footer += f" | Cached at {cached_at}"

    return f"""
    <section style="background:#ffffff;border:1px solid #e2e8f0;border-radius:20px;padding:20px 22px;box-shadow:0 8px 24px rgba(15,23,42,0.05);">
      <div style="font-size:13px;font-weight:800;letter-spacing:0.08em;text-transform:uppercase;color:#64748b;margin-bottom:12px;">利率 / Rates</div>
      <div style="display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px;">
        <div style="background:#f8fafc;border-radius:14px;padding:14px 16px;">
          <div style="font-size:12px;color:#64748b;margin-bottom:6px;">US 2Y</div>
          <div style="font-size:24px;font-weight:800;color:#0f172a;">{safe_escape(us_2y_display)}</div>
          <div style="font-size:13px;color:{change_color(us_2y.get('change_bps'))};">{safe_escape(format_signed(us_2y.get('change_bps'), suffix='bp'))}</div>
        </div>
        <div style="background:#f8fafc;border-radius:14px;padding:14px 16px;">
          <div style="font-size:12px;color:#64748b;margin-bottom:6px;">US 10Y</div>
          <div style="font-size:24px;font-weight:800;color:#0f172a;">{safe_escape(us_10y_display)}</div>
          <div style="font-size:13px;color:{change_color(us_10y.get('change_bps'))};">{safe_escape(format_signed(us_10y.get('change_bps'), suffix='bp'))}</div>
        </div>
        <div style="background:#f8fafc;border-radius:14px;padding:14px 16px;">
          <div style="font-size:12px;color:#64748b;margin-bottom:6px;">10Y-2Y Curve</div>
          <div style="font-size:24px;font-weight:800;color:#0f172a;">{safe_escape(format_signed(curve, suffix='bp'))}</div>
          <div style="font-size:13px;color:#64748b;">As of {safe_escape(rates.get('as_of_date'), 'n/a')}</div>
        </div>
      </div>
      <div style="margin-top:10px;font-size:11px;color:#94a3b8;">{safe_escape(footer)}</div>
    </section>
    """


def build_table_card(title, items):
    return f"""
    <section style="background:#ffffff;border:1px solid #e2e8f0;border-radius:20px;padding:20px 22px;box-shadow:0 8px 24px rgba(15,23,42,0.05);">
      <div style="font-size:13px;font-weight:800;letter-spacing:0.08em;text-transform:uppercase;color:#64748b;margin-bottom:12px;">{escape(title)}</div>
      <table style="width:100%;border-collapse:collapse;">
        <thead>
          <tr>
            <th align="left" style="padding:0 12px 10px;color:#64748b;font-size:12px;text-transform:uppercase;">Asset</th>
            <th align="left" style="padding:0 12px 10px;color:#64748b;font-size:12px;text-transform:uppercase;">Level</th>
            <th align="left" style="padding:0 12px 10px;color:#64748b;font-size:12px;text-transform:uppercase;">Chg</th>
            <th align="left" style="padding:0 12px 10px;color:#64748b;font-size:12px;text-transform:uppercase;">Chg %</th>
          </tr>
        </thead>
        <tbody>
          {build_market_rows(items)}
        </tbody>
      </table>
    </section>
    """


def build_signal_cards(report):
    cards = []
    for index, item in enumerate(report.get("top_signals", []), start=1):
        module = safe_text(item.get("module"), "Macro/Geopolitics")
        cards.append(
            f"""
            <section style="background:#ffffff;border:1px solid #e2e8f0;border-radius:20px;padding:20px 22px;box-shadow:0 8px 24px rgba(15,23,42,0.05);">
              <div style="display:flex;justify-content:space-between;gap:12px;align-items:flex-start;margin-bottom:12px;flex-wrap:wrap;">
                <div style="font-size:13px;font-weight:800;letter-spacing:0.08em;text-transform:uppercase;color:#64748b;">Signal #{index}</div>
                <span style="background:#e2e8f0;border-radius:999px;padding:6px 10px;font-size:12px;font-weight:700;color:#334155;">{safe_escape(module_to_zh(module), module)} / {safe_escape(module)}</span>
              </div>
              <h3 style="margin:0 0 8px;font-size:24px;line-height:1.35;color:#0f172a;">{safe_escape(item.get('signal_zh'), item.get('signal'))}</h3>
              <p style="margin:0 0 12px;color:#64748b;font-size:13px;line-height:1.7;">{safe_escape(item.get('signal'), 'No clear signal extracted.')}</p>
              <p style="margin:0 0 6px;color:#334155;font-size:14px;line-height:1.8;"><strong style="color:#0f172a;">为什么重要：</strong>{safe_escape(item.get('why_it_matters_zh'), item.get('why_it_matters'))}</p>
              <p style="margin:0 0 10px;color:#64748b;font-size:13px;line-height:1.7;">Why it matters: {safe_escape(item.get('why_it_matters'), 'Why it matters was not provided.')}</p>
              <p style="margin:0 0 6px;color:#334155;font-size:14px;line-height:1.8;"><strong style="color:#0f172a;">市场影响：</strong>{safe_escape(item.get('market_impact_zh'), item.get('market_impact'))}</p>
              <p style="margin:0;color:#64748b;font-size:13px;line-height:1.7;">Market impact: {safe_escape(item.get('market_impact'), 'Cross-asset impact was not provided.')}</p>
            </section>
            """
        )
    if not cards:
        cards.append(
            """
            <section style="background:#ffffff;border:1px solid #e2e8f0;border-radius:20px;padding:20px 22px;box-shadow:0 8px 24px rgba(15,23,42,0.05);">
              <div style="font-size:13px;font-weight:800;letter-spacing:0.08em;text-transform:uppercase;color:#64748b;margin-bottom:12px;">Signals</div>
              <p style="margin:0;color:#334155;font-size:14px;line-height:1.7;">No high-confidence cross-asset signal was extracted from the current input set.</p>
            </section>
            """
        )
    return "".join(cards)


def build_module_cards(report):
    cards = []
    for module_key in ["macro_geopolitics", "commodities", "rates", "equities", "fx"]:
        module = report.get("modules", {}).get(module_key, {})
        label = safe_text(module.get("label"), module_key)
        cards.append(
            f"""
            <section style="background:#ffffff;border:1px solid #e2e8f0;border-radius:18px;padding:18px 20px;box-shadow:0 8px 24px rgba(15,23,42,0.05);">
              <div style="font-size:13px;font-weight:800;letter-spacing:0.08em;text-transform:uppercase;color:#64748b;margin-bottom:10px;">{safe_escape(module_to_zh(label), label)} / {safe_escape(label)}</div>
              <p style="margin:0 0 8px;color:#1e293b;font-size:14px;line-height:1.8;">{safe_escape(module.get('summary_zh'), module.get('summary'))}</p>
              <p style="margin:0 0 10px;color:#64748b;font-size:13px;line-height:1.7;">{safe_escape(module.get('summary'), 'No strong signal detected.')}</p>
              <p style="margin:0 0 4px;color:#475569;font-size:13px;line-height:1.8;"><strong style="color:#0f172a;">关注点：</strong>{safe_escape(module.get('watch_zh'), module.get('watch'))}</p>
              <p style="margin:0;color:#94a3b8;font-size:12px;line-height:1.7;">Watch: {safe_escape(module.get('watch'), 'No specific watch item provided.')}</p>
            </section>
            """
        )
    return "".join(cards)


def build_watchlist(items):
    if not items:
        return "<li>No explicit watchlist was provided.</li>"
    return "".join(f"<li>{safe_escape(item)}</li>" for item in items)


def build_macro_email(report, market_snapshot, config):
    local_now = datetime.now(ZoneInfo(config["local_timezone"]))
    regime = safe_text(report.get("regime"), "Mixed")
    subject = f"宏观日报 | {regime_to_zh(regime)} | {local_now.strftime('%Y-%m-%d')}"

    html = f"""
    <html>
      <body style="margin:0;padding:0;background:#e7ecf3;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#0f172a;">
        <div style="max-width:1020px;margin:0 auto;padding:28px 18px 40px;">
          <header style="background:linear-gradient(135deg,#111827 0%,#1d4ed8 55%,#0f766e 100%);border-radius:28px;padding:30px 30px 24px;color:#ffffff;box-shadow:0 18px 50px rgba(15,23,42,0.24);margin-bottom:22px;">
            <div style="display:flex;justify-content:space-between;gap:16px;align-items:flex-start;flex-wrap:wrap;margin-bottom:16px;">
              <div>
                <div style="font-size:13px;font-weight:800;letter-spacing:0.08em;text-transform:uppercase;opacity:0.8;margin-bottom:10px;">Macro Signal Extractor / 宏观信号压缩器</div>
                <h1 style="margin:0 0 10px;font-size:34px;line-height:1.15;max-width:760px;">{safe_escape(report.get('headline_zh'), report.get('headline'))}</h1>
                <div style="font-size:15px;line-height:1.7;opacity:0.82;max-width:760px;">{safe_escape(report.get('headline'), 'Macro daily brief')}</div>
              </div>
              <div style="background:rgba(255,255,255,0.16);border:1px solid rgba(255,255,255,0.24);padding:10px 14px;border-radius:999px;font-size:14px;font-weight:800;white-space:nowrap;">
                {safe_escape(regime_to_zh(regime), regime)} / {safe_escape(regime)}
              </div>
            </div>
            <p style="margin:0 0 10px;font-size:16px;line-height:1.9;max-width:780px;opacity:0.98;">{safe_escape(report.get('cross_asset_take_zh'), report.get('cross_asset_take'))}</p>
            <p style="margin:0;font-size:13px;line-height:1.8;max-width:780px;opacity:0.8;">{safe_escape(report.get('cross_asset_take'), 'No cross-asset view was produced.')}</p>
          </header>

          <div style="display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:18px;margin-bottom:18px;">
            {build_table_card("大宗商品 / Commodities", market_snapshot.get("commodities", []))}
            {build_rates_card(market_snapshot.get("rates", {}))}
            {build_table_card("板块 / Sector Equities", market_snapshot.get("equities", []))}
            {build_table_card("外汇 / FX", market_snapshot.get("fx", []))}
          </div>

          <div style="display:grid;grid-template-columns:1fr;gap:18px;margin-bottom:18px;">
            {build_signal_cards(report)}
          </div>

          <section style="background:#ffffff;border:1px solid #e2e8f0;border-radius:20px;padding:22px 24px;box-shadow:0 8px 24px rgba(15,23,42,0.05);margin-bottom:18px;">
            <div style="font-size:13px;font-weight:800;letter-spacing:0.08em;text-transform:uppercase;color:#64748b;margin-bottom:12px;">模块总结 / Module Takeaways</div>
            <div style="display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:16px;">
              {build_module_cards(report)}
            </div>
          </section>

          <section style="background:#ffffff;border:1px solid #e2e8f0;border-radius:20px;padding:22px 24px;box-shadow:0 8px 24px rgba(15,23,42,0.05);">
            <div style="font-size:13px;font-weight:800;letter-spacing:0.08em;text-transform:uppercase;color:#64748b;margin-bottom:12px;">明日观察 / Tomorrow Watchlist</div>
            <div style="display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:16px;">
              <div>
                <div style="font-size:12px;font-weight:800;letter-spacing:0.06em;text-transform:uppercase;color:#94a3b8;margin-bottom:8px;">中文</div>
                <ul style="margin:0;padding-left:20px;color:#1e293b;font-size:14px;line-height:1.8;">
                  {build_watchlist(report.get('tomorrow_watchlist_zh', []))}
                </ul>
              </div>
              <div>
                <div style="font-size:12px;font-weight:800;letter-spacing:0.06em;text-transform:uppercase;color:#94a3b8;margin-bottom:8px;">English</div>
                <ul style="margin:0;padding-left:20px;color:#64748b;font-size:13px;line-height:1.8;">
                  {build_watchlist(report.get('tomorrow_watchlist', []))}
                </ul>
              </div>
            </div>
          </section>

          <section style="margin-top:16px;color:#64748b;font-size:12px;line-height:1.8;padding:0 4px;">
            <div>行情源优先级：默认优先使用 yfinance 拉全市场快照；缺口再回退到 Stooq 与 Frankfurter；DXY 优先使用 yfinance 的 `DX-Y.NYB`，否则按 6 币种权重公式推导；Yahoo 原生接口默认关闭；国债源优先级：FRED -> Treasury 官方 CSV -> Treasury 当前月页面 -> Treasury 通用页面 -> 本地缓存。</div>
            <div>Market source priority: yfinance is used first for the full market snapshot by default; gaps then fall back to Stooq and Frankfurter; DXY prefers yfinance `DX-Y.NYB`, otherwise it is derived from the six-currency weighted formula; native Yahoo endpoints remain disabled by default; rates source priority: FRED -> Treasury official CSV -> Treasury current-month page -> generic Treasury page -> local cache.</div>
          </section>
        </div>
      </body>
    </html>
    """
    return subject, html
