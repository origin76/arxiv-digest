import smtplib
import ssl
import time
from email.mime.text import MIMEText
from html import escape

from digest_runtime import LOGGER


def score_to_color(score):
    if score >= 90:
        return "#0f766e"
    if score >= 80:
        return "#2563eb"
    if score >= 70:
        return "#7c3aed"
    return "#6b7280"


def build_email(papers):
    cards = []
    for index, paper in enumerate(papers, start=1):
        score_color = score_to_color(paper["score"])
        summary_items = "".join(
            f"<li>{escape(item)}</li>"
            for item in paper["summary"]
        )
        cards.append(
            f"""
            <section style="background:#ffffff;border:1px solid #e5e7eb;border-radius:20px;padding:24px 24px 20px;margin:0 0 18px;box-shadow:0 10px 30px rgba(15,23,42,0.06);">
              <div style="display:flex;justify-content:space-between;gap:16px;align-items:flex-start;flex-wrap:wrap;margin-bottom:14px;">
                <div style="flex:1;min-width:280px;">
                  <div style="font-size:12px;font-weight:700;letter-spacing:0.08em;text-transform:uppercase;color:#64748b;margin-bottom:10px;">Rank #{index}</div>
                  <h2 style="margin:0;font-size:24px;line-height:1.3;color:#0f172a;">{escape(paper['title'])}</h2>
                </div>
                <div style="background:{score_color};color:#ffffff;border-radius:999px;padding:10px 14px;font-size:14px;font-weight:700;white-space:nowrap;">
                  Score {paper['score']}/100
                </div>
              </div>

              <div style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:16px;">
                <span style="background:#eff6ff;color:#1d4ed8;border-radius:999px;padding:6px 10px;font-size:12px;font-weight:700;">{escape(paper['fit_area'])}</span>
                <span style="background:#f8fafc;color:#475569;border-radius:999px;padding:6px 10px;font-size:12px;font-weight:600;">OS / AI-Infra / Compiler digest</span>
              </div>

              <p style="margin:0 0 10px;color:#334155;font-size:14px;line-height:1.7;"><strong style="color:#0f172a;">Authors:</strong> {escape(paper['authors_display'])}</p>
              <p style="margin:0 0 10px;color:#334155;font-size:14px;line-height:1.7;"><strong style="color:#0f172a;">Why Read:</strong> {escape(paper['reason'])}</p>
              <p style="margin:0 0 18px;color:#334155;font-size:14px;line-height:1.7;"><strong style="color:#0f172a;">Affiliation Signal:</strong> {escape(paper['affiliation_signal'])}</p>

              <div style="background:#f8fafc;border-radius:16px;padding:16px 18px;margin-bottom:16px;">
                <div style="font-size:13px;font-weight:800;letter-spacing:0.04em;text-transform:uppercase;color:#475569;margin-bottom:10px;">Key Points</div>
                <ul style="margin:0;padding-left:20px;color:#1e293b;font-size:14px;line-height:1.75;">
                  {summary_items}
                </ul>
              </div>

              <div style="background:linear-gradient(135deg,#fff7ed 0%,#fffbeb 100%);border:1px solid #fed7aa;border-radius:16px;padding:16px 18px;margin-bottom:16px;">
                <div style="font-size:13px;font-weight:800;letter-spacing:0.04em;text-transform:uppercase;color:#9a3412;margin-bottom:8px;">中文速览</div>
                <p style="margin:0;color:#7c2d12;font-size:14px;line-height:1.75;">{escape(paper['translation'])}</p>
              </div>

              <a href="{escape(paper['link'])}" style="display:inline-block;background:#111827;color:#ffffff;text-decoration:none;padding:11px 16px;border-radius:12px;font-size:14px;font-weight:700;">Read on arXiv</a>
            </section>
            """
        )

    return f"""
    <html>
      <body style="margin:0;padding:0;background:#f1f5f9;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#0f172a;">
        <div style="max-width:920px;margin:0 auto;padding:32px 18px 40px;">
          <header style="background:linear-gradient(135deg,#0f172a 0%,#1d4ed8 60%,#0f766e 100%);border-radius:28px;padding:28px 28px 24px;color:#ffffff;box-shadow:0 18px 60px rgba(15,23,42,0.22);margin-bottom:22px;">
            <div style="font-size:13px;font-weight:800;letter-spacing:0.08em;text-transform:uppercase;opacity:0.82;margin-bottom:10px;">Daily Research Digest</div>
            <h1 style="margin:0 0 10px;font-size:34px;line-height:1.15;">Top {len(papers)} Papers For OS, AI Infra, AI Compilers, and Program Analysis</h1>
            <p style="margin:0;font-size:16px;line-height:1.7;max-width:680px;opacity:0.92;">
              Ranked by overall quality and worth-reading score within the digest scope, using abstract as the primary signal and author affiliations as a secondary confidence signal.
            </p>
          </header>
          {''.join(cards)}
        </div>
      </body>
    </html>
    """


def send_email(html, smtp_config):
    recipients = [item.strip() for item in smtp_config["to"].split(",") if item.strip()]
    msg = MIMEText(html, "html", "utf-8")
    msg["Subject"] = "Top 10: OS, AI Infra, AI Compilers, Program Analysis"
    msg["From"] = smtp_config["user"]
    msg["To"] = ", ".join(recipients)

    smtp_class = smtplib.SMTP_SSL if smtp_config["use_ssl"] else smtplib.SMTP

    LOGGER.info(
        "Sending email | host=%s port=%s recipients=%d use_ssl=%s use_starttls=%s",
        smtp_config["host"],
        smtp_config["port"],
        len(recipients),
        smtp_config["use_ssl"],
        smtp_config["use_starttls"],
    )
    start_time = time.perf_counter()

    with smtp_class(smtp_config["host"], smtp_config["port"]) as server:
        if smtp_config["use_starttls"]:
            server.ehlo()
            server.starttls(context=ssl.create_default_context())
            server.ehlo()
        server.login(smtp_config["user"], smtp_config["password"])
        server.send_message(msg)

    duration = time.perf_counter() - start_time
    LOGGER.info("Email sent successfully | duration=%.2fs recipients=%s", duration, recipients)
