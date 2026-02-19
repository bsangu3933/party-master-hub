import { useState, useEffect, useCallback } from "react";
import { BarChart, Bar, PieChart, Pie, Cell, XAxis, YAxis, Tooltip, ResponsiveContainer, Legend } from "recharts";

// ============================================================
// CONFIG
// ============================================================
const API_BASE = "http://localhost:8000";
const REFRESH_INTERVAL = 30000; // 30 seconds

// ============================================================
// DESIGN SYSTEM — Industrial/Utilitarian Data Terminal
// ============================================================
const colors = {
  bg:        "#0a0c0f",
  panel:     "#0f1215",
  border:    "#1e2530",
  accent:    "#00d4ff",
  accent2:   "#00ff9d",
  accent3:   "#ff6b35",
  warning:   "#ffd166",
  error:     "#ff4757",
  success:   "#00ff9d",
  muted:     "#3a4556",
  text:      "#c8d6e5",
  textDim:   "#5a6a7e",
};

const CHART_COLORS = ["#00d4ff", "#00ff9d", "#ff6b35", "#ffd166", "#a29bfe", "#fd79a8"];

const PIPELINE_DEPS = {
  generate_data: [],
  ingest:        ["generate_data"],
  master:        ["ingest"],
  quality:       ["master"],
  export_s3:     ["master"],
};

const STEP_LABELS = {
  generate_data: "Generate Data",
  ingest:        "Ingest",
  master:        "Master",
  quality:       "Quality",
  export_s3:     "Export S3",
};

const STEP_ICONS = {
  generate_data: "⚙",
  ingest:        "⬇",
  master:        "◈",
  quality:       "✓",
  export_s3:     "↑",
};

// ============================================================
// STYLES
// ============================================================
const styles = {
  app: {
    background:  colors.bg,
    minHeight:   "100vh",
    fontFamily:  "'IBM Plex Mono', 'Courier New', monospace",
    color:       colors.text,
    padding:     "0",
  },
  header: {
    background:    colors.panel,
    borderBottom:  `1px solid ${colors.border}`,
    padding:       "16px 28px",
    display:       "flex",
    alignItems:    "center",
    justifyContent:"space-between",
    position:      "sticky",
    top:           0,
    zIndex:        100,
  },
  headerLeft: {
    display:    "flex",
    alignItems: "center",
    gap:        "16px",
  },
  logo: {
    width:        "36px",
    height:       "36px",
    background:   `linear-gradient(135deg, ${colors.accent}, ${colors.accent2})`,
    borderRadius: "4px",
    display:      "flex",
    alignItems:   "center",
    justifyContent:"center",
    fontSize:     "18px",
    fontWeight:   "bold",
    color:        colors.bg,
  },
  headerTitle: {
    fontSize:    "14px",
    fontWeight:  "600",
    color:       colors.text,
    letterSpacing:"0.08em",
    textTransform:"uppercase",
  },
  headerSub: {
    fontSize:  "11px",
    color:     colors.textDim,
    marginTop: "2px",
  },
  badge: {
    background:   "rgba(0,212,255,0.1)",
    border:       `1px solid ${colors.accent}`,
    color:        colors.accent,
    padding:      "3px 10px",
    borderRadius: "3px",
    fontSize:     "11px",
    letterSpacing:"0.05em",
  },
  main: {
    padding: "24px 28px",
    maxWidth:"1600px",
    margin:  "0 auto",
  },
  grid2: {
    display:             "grid",
    gridTemplateColumns: "1fr 1fr",
    gap:                 "16px",
    marginBottom:        "16px",
  },
  grid3: {
    display:             "grid",
    gridTemplateColumns: "1fr 1fr 1fr",
    gap:                 "16px",
    marginBottom:        "16px",
  },
  grid4: {
    display:             "grid",
    gridTemplateColumns: "repeat(4, 1fr)",
    gap:                 "16px",
    marginBottom:        "16px",
  },
  panel: {
    background:   colors.panel,
    border:       `1px solid ${colors.border}`,
    borderRadius: "6px",
    overflow:     "hidden",
  },
  panelHeader: {
    padding:       "12px 16px",
    borderBottom:  `1px solid ${colors.border}`,
    display:       "flex",
    alignItems:    "center",
    justifyContent:"space-between",
  },
  panelTitle: {
    fontSize:     "11px",
    fontWeight:   "600",
    color:        colors.textDim,
    letterSpacing:"0.1em",
    textTransform:"uppercase",
  },
  panelBody: {
    padding: "16px",
  },
  statCard: {
    background:   colors.panel,
    border:       `1px solid ${colors.border}`,
    borderRadius: "6px",
    padding:      "16px",
    position:     "relative",
    overflow:     "hidden",
  },
  statValue: {
    fontSize:   "32px",
    fontWeight: "700",
    color:      colors.accent,
    lineHeight: "1",
    fontVariantNumeric: "tabular-nums",
  },
  statLabel: {
    fontSize:  "11px",
    color:     colors.textDim,
    marginTop: "6px",
    textTransform:"uppercase",
    letterSpacing:"0.08em",
  },
  statAccent: {
    position:   "absolute",
    top:        0,
    left:       0,
    width:      "3px",
    height:     "100%",
    background: colors.accent,
  },
  tag: (color) => ({
    display:      "inline-block",
    padding:      "2px 8px",
    borderRadius: "3px",
    fontSize:     "10px",
    fontWeight:   "600",
    letterSpacing:"0.05em",
    textTransform:"uppercase",
    background:   `${color}20`,
    color:        color,
    border:       `1px solid ${color}40`,
  }),
  btn: (color = colors.accent) => ({
    background:    "transparent",
    border:        `1px solid ${color}`,
    color:         color,
    padding:       "5px 14px",
    borderRadius:  "3px",
    fontSize:      "11px",
    cursor:        "pointer",
    fontFamily:    "inherit",
    letterSpacing: "0.05em",
    textTransform: "uppercase",
    transition:    "all 0.15s",
  }),
  pipelineFlow: {
    display:    "flex",
    alignItems: "center",
    gap:        "0",
    padding:    "20px 16px",
    overflowX:  "auto",
  },
  pipelineStep: (status) => ({
    display:       "flex",
    flexDirection: "column",
    alignItems:    "center",
    minWidth:      "140px",
    position:      "relative",
  }),
  pipelineNode: (status) => ({
    width:        "60px",
    height:       "60px",
    borderRadius: "8px",
    display:      "flex",
    alignItems:   "center",
    justifyContent:"center",
    fontSize:     "22px",
    border:       `2px solid ${
      status === 'success' ? colors.accent2 :
      status === 'running' ? colors.warning :
      status === 'error'   ? colors.error :
      colors.muted
    }`,
    background:   `${
      status === 'success' ? colors.accent2 :
      status === 'running' ? colors.warning :
      status === 'error'   ? colors.error :
      colors.muted
    }15`,
    boxShadow:    status === 'success' ? `0 0 20px ${colors.accent2}30` :
                  status === 'running' ? `0 0 20px ${colors.warning}30` : 'none',
    cursor:       "pointer",
    transition:   "all 0.2s",
  }),
  pipelineArrow: {
    flex:      "1",
    height:    "2px",
    background:`linear-gradient(90deg, ${colors.accent2}40, ${colors.accent}40)`,
    minWidth:  "20px",
    position:  "relative",
  },
  tableRow: {
    display:       "grid",
    padding:       "8px 0",
    borderBottom:  `1px solid ${colors.border}`,
    fontSize:      "12px",
    alignItems:    "center",
  },
  scrollBox: {
    maxHeight: "260px",
    overflowY: "auto",
  },
  refreshBar: {
    height:     "2px",
    background: `linear-gradient(90deg, ${colors.accent}, ${colors.accent2})`,
    transition: "width 30s linear",
  },
  dot: (color) => ({
    width:        "8px",
    height:       "8px",
    borderRadius: "50%",
    background:   color,
    display:      "inline-block",
    marginRight:  "6px",
    boxShadow:    `0 0 6px ${color}`,
  }),
};

// ============================================================
// HOOKS
// ============================================================
function useAPI(endpoint, interval = null) {
  const [data, setData]     = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError]   = useState(null);

  const fetch_ = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}${endpoint}`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      setData(json);
      setError(null);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, [endpoint]);

  useEffect(() => {
    fetch_();
    if (interval) {
      const id = setInterval(fetch_, interval);
      return () => clearInterval(id);
    }
  }, [fetch_, interval]);

  return { data, loading, error, refetch: fetch_ };
}

// ============================================================
// COMPONENTS
// ============================================================

function StatusDot({ status }) {
  const color =
    status === "success" ? colors.success :
    status === "running" ? colors.warning :
    status === "error"   ? colors.error   :
    colors.muted;
  return <span style={styles.dot(color)} />;
}

function Tag({ children, color = colors.accent }) {
  return <span style={styles.tag(color)}>{children}</span>;
}

function Panel({ title, children, action, fullWidth }) {
  return (
    <div style={{ ...styles.panel, gridColumn: fullWidth ? "1/-1" : undefined }}>
      <div style={styles.panelHeader}>
        <span style={styles.panelTitle}>{title}</span>
        {action}
      </div>
      <div style={styles.panelBody}>{children}</div>
    </div>
  );
}

function StatCard({ value, label, color = colors.accent, sub }) {
  return (
    <div style={styles.statCard}>
      <div style={{ ...styles.statAccent, background: color }} />
      <div style={{ paddingLeft: "12px" }}>
        <div style={{ ...styles.statValue, color }}>{value ?? "—"}</div>
        <div style={styles.statLabel}>{label}</div>
        {sub && <div style={{ fontSize: "11px", color: colors.textDim, marginTop: "4px" }}>{sub}</div>}
      </div>
    </div>
  );
}

// ============================================================
// PIPELINE FLOW
// ============================================================
function PipelineFlow({ status, onTrigger, triggerStatus }) {
  const steps = Object.keys(PIPELINE_DEPS);

  return (
    <Panel title="▸ Pipeline Flow" fullWidth>
      <div style={styles.pipelineFlow}>
        {steps.map((step, i) => {
          const s = status?.pipeline_steps?.[step];
          const st = s?.status || "unknown";
          const isRunning = triggerStatus[step] === "running";
          const displayStatus = isRunning ? "running" : st;

          return (
            <div key={step} style={{ display: "flex", alignItems: "center", flex: 1 }}>
              <div style={styles.pipelineStep(displayStatus)}>
                <div
                  style={styles.pipelineNode(displayStatus)}
                  onClick={() => onTrigger(step)}
                  title={`Click to run: ${STEP_LABELS[step]}`}
                >
                  {isRunning ? "⟳" : STEP_ICONS[step]}
                </div>
                <div style={{ marginTop: "8px", fontSize: "11px", color: colors.text, textAlign: "center", fontWeight: "600" }}>
                  {STEP_LABELS[step]}
                </div>
                <div style={{ fontSize: "10px", color: colors.textDim, marginTop: "2px", textAlign: "center" }}>
                  {isRunning ? "Running..." :
                   s?.age_hours != null ? `${s.age_hours}h ago` :
                   "Not run"}
                </div>
                {s?.details && (
                  <div style={{ fontSize: "10px", color: colors.textDim, marginTop: "2px", textAlign: "center", maxWidth: "120px" }}>
                    {s.details}
                  </div>
                )}
                <div style={{ marginTop: "6px" }}>
                  {displayStatus === "success" && <Tag color={colors.success}>✓ OK</Tag>}
                  {displayStatus === "running" && <Tag color={colors.warning}>Running</Tag>}
                  {displayStatus === "error"   && <Tag color={colors.error}>Error</Tag>}
                  {displayStatus === "not_run" && <Tag color={colors.muted}>Not Run</Tag>}
                  {displayStatus === "unknown" && <Tag color={colors.muted}>Unknown</Tag>}
                </div>
              </div>
              {i < steps.length - 1 && (
                <div style={styles.pipelineArrow}>
                  <div style={{ position: "absolute", right: "-6px", top: "-5px", color: colors.accent, fontSize: "12px" }}>▶</div>
                </div>
              )}
            </div>
          );
        })}
      </div>
      <div style={{ padding: "0 16px 8px", fontSize: "11px", color: colors.textDim }}>
        💡 Click any step node to trigger it. Steps run as background tasks.
      </div>
    </Panel>
  );
}

// ============================================================
// POSTGRES PANEL
// ============================================================
function PostgresPanel({ stats }) {
  const s = stats?.stats;
  if (!s) return <Panel title="▸ PostgreSQL"><div style={{ color: colors.textDim, fontSize: "12px" }}>Loading...</div></Panel>;

  const sources = s.source_parties?.rows || [];
  const bySource = {};
  sources.forEach(r => {
    bySource[r.source_system] = (bySource[r.source_system] || 0) + Number(r.count);
  });

  return (
    <Panel title="▸ PostgreSQL — Table Stats">
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "10px", marginBottom: "16px" }}>
        <StatCard value={s.source_parties?.total} label="Source Records" color={colors.accent} />
        <StatCard value={s.golden_parties?.total} label="Golden Records" color={colors.accent2} />
        <StatCard value={s.party_clusters?.duplicates_resolved} label="Duplicates Resolved" color={colors.accent3} />
        <StatCard value={s.party_lineage?.total} label="Lineage Events" color={colors.warning} />
      </div>

      <div style={{ fontSize: "11px", color: colors.textDim, marginBottom: "8px", textTransform: "uppercase", letterSpacing: "0.08em" }}>
        Source Breakdown
      </div>
      <div style={styles.scrollBox}>
        {Object.entries(bySource).map(([source, count]) => (
          <div key={source} style={{ ...styles.tableRow, gridTemplateColumns: "1fr auto" }}>
            <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
              <StatusDot status="success" />
              <span style={{ fontSize: "12px" }}>{source}</span>
            </div>
            <Tag color={colors.accent}>{count}</Tag>
          </div>
        ))}
      </div>

      <div style={{ fontSize: "11px", color: colors.textDim, marginTop: "14px", marginBottom: "8px", textTransform: "uppercase", letterSpacing: "0.08em" }}>
        Recent Lineage Events
      </div>
      <div style={styles.scrollBox}>
        {(s.party_lineage?.rows || []).map((row, i) => (
          <div key={i} style={{ ...styles.tableRow, gridTemplateColumns: "1fr auto" }}>
            <span style={{ fontSize: "11px", color: colors.textDim }}>{row.event_type}</span>
            <Tag color={
              row.event_type === "CREATED"         ? colors.accent2 :
              row.event_type === "ADDRESS_UPDATED" ? colors.accent  :
              row.event_type === "MERGED"          ? colors.accent3 :
              colors.warning
            }>{row.count}</Tag>
          </div>
        ))}
      </div>
    </Panel>
  );
}

// ============================================================
// S3 PANEL
// ============================================================
function S3Panel({ files }) {
  const [preview, setPreview] = useState(null);
  const [previewData, setPreviewData] = useState(null);

  const handlePreview = async (zone, key) => {
    setPreview(key);
    try {
      const res = await fetch(`${API_BASE}/system/s3/preview/${zone}/${key}`);
      const data = await res.json();
      setPreviewData(data);
    } catch (e) {
      setPreviewData({ error: e.message });
    }
  };

  if (!files) return <Panel title="▸ S3 Explorer"><div style={{ color: colors.textDim, fontSize: "12px" }}>Loading...</div></Panel>;

  return (
    <Panel title="▸ S3 Explorer">
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: "10px", marginBottom: "16px" }}>
        {Object.entries(files.zones || {}).map(([zone, info]) => (
          <div key={zone} style={{ background: `${colors.accent}08`, border: `1px solid ${colors.border}`, borderRadius: "4px", padding: "10px" }}>
            <div style={{ fontSize: "10px", color: colors.textDim, textTransform: "uppercase", letterSpacing: "0.08em" }}>{zone} zone</div>
            <div style={{ fontSize: "20px", fontWeight: "700", color: colors.accent, marginTop: "4px" }}>{info.file_count || 0}</div>
            <div style={{ fontSize: "11px", color: colors.textDim }}>{info.total_size_kb || 0} KB</div>
          </div>
        ))}
      </div>

      <div style={{ fontSize: "11px", color: colors.textDim, marginBottom: "8px", textTransform: "uppercase", letterSpacing: "0.08em" }}>
        Files — click to preview
      </div>
      <div style={styles.scrollBox}>
        {Object.entries(files.zones || {}).map(([zone, info]) =>
          (info.files || []).map((file, i) => (
            <div
              key={i}
              style={{ ...styles.tableRow, gridTemplateColumns: "1fr auto auto", cursor: "pointer", gap: "8px" }}
              onClick={() => handlePreview(zone, file.key)}
            >
              <div style={{ fontSize: "11px", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                <span style={{ color: colors.accent }}>s3://</span>{file.bucket}/{file.key}
              </div>
              <span style={{ fontSize: "11px", color: colors.textDim, whiteSpace: "nowrap" }}>{file.size_kb} KB</span>
              <Tag color={zone === 'golden' ? colors.accent2 : zone === 'raw' ? colors.accent3 : colors.accent}>{zone}</Tag>
            </div>
          ))
        )}
      </div>

      {preview && previewData && (
        <div style={{ marginTop: "16px", background: colors.bg, border: `1px solid ${colors.border}`, borderRadius: "4px", padding: "12px" }}>
          <div style={{ fontSize: "11px", color: colors.accent, marginBottom: "8px" }}>
            Preview: {preview} ({previewData.total_rows} rows)
          </div>
          {previewData.preview && (
            <div style={{ overflowX: "auto" }}>
              <table style={{ borderCollapse: "collapse", fontSize: "11px", width: "100%" }}>
                <thead>
                  <tr>
                    {(previewData.columns || []).slice(0, 6).map(col => (
                      <th key={col} style={{ padding: "4px 8px", color: colors.textDim, textAlign: "left", borderBottom: `1px solid ${colors.border}`, whiteSpace: "nowrap" }}>{col}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {(previewData.preview || []).slice(0, 5).map((row, i) => (
                    <tr key={i}>
                      {(previewData.columns || []).slice(0, 6).map(col => (
                        <td key={col} style={{ padding: "4px 8px", color: colors.text, borderBottom: `1px solid ${colors.border}22`, whiteSpace: "nowrap", maxWidth: "150px", overflow: "hidden", textOverflow: "ellipsis" }}>
                          {String(row[col] || "")}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </Panel>
  );
}

// ============================================================
// KAFKA PANEL
// ============================================================
function KafkaPanel({ kafka }) {
  if (!kafka) return <Panel title="▸ Kafka Monitor"><div style={{ color: colors.textDim, fontSize: "12px" }}>Loading...</div></Panel>;

  const group = kafka.consumer_groups?.["party-master-consumer"] || {};
  const topics = kafka.topics || {};

  return (
    <Panel title="▸ Kafka Monitor">
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "10px", marginBottom: "16px" }}>
        <StatCard
          value={topics[Object.keys(topics)[0]]?.total_messages ?? "—"}
          label="Total Messages"
          color={colors.accent}
        />
        <StatCard
          value={group.lag ?? "—"}
          label="Consumer Lag"
          color={group.lag === 0 ? colors.success : colors.warning}
          sub={group.status}
        />
      </div>

      <div style={{ fontSize: "11px", color: colors.textDim, marginBottom: "8px", textTransform: "uppercase", letterSpacing: "0.08em" }}>
        Topics
      </div>
      {Object.entries(topics).map(([topic, info]) => (
        <div key={topic} style={{ ...styles.tableRow, gridTemplateColumns: "1fr auto" }}>
          <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
            <StatusDot status={info.status === "available" ? "success" : "error"} />
            <span style={{ fontSize: "11px" }}>{topic}</span>
          </div>
          <Tag color={colors.accent}>{info.partitions || 0} partitions</Tag>
        </div>
      ))}

      <div style={{ fontSize: "11px", color: colors.textDim, marginTop: "14px", marginBottom: "8px", textTransform: "uppercase", letterSpacing: "0.08em" }}>
        Recent Events (via Lineage)
      </div>
      <div style={styles.scrollBox}>
        {(kafka.recent_events || []).length === 0 && (
          <div style={{ fontSize: "11px", color: colors.textDim }}>No Kafka-triggered events yet. Run the producer.</div>
        )}
        {(kafka.recent_events || []).map((evt, i) => (
          <div key={i} style={{ ...styles.tableRow, gridTemplateColumns: "1fr auto auto" }}>
            <span style={{ fontSize: "11px", color: colors.textDim, overflow: "hidden", textOverflow: "ellipsis" }}>
              {evt.golden_id?.slice(0, 16)}...
            </span>
            <Tag color={colors.accent}>{evt.event_type}</Tag>
          </div>
        ))}
      </div>
    </Panel>
  );
}

// ============================================================
// CHARTS PANEL
// ============================================================
function ChartsPanel({ charts }) {
  if (!charts) return null;
  const c = charts.charts || {};

  return (
    <>
      {/* Summary Stats */}
      <div style={styles.grid4}>
        <StatCard value={c.summary_stats?.total_source}     label="Source Records"      color={colors.accent} />
        <StatCard value={c.summary_stats?.total_golden}     label="Golden Records"      color={colors.accent2} />
        <StatCard value={c.summary_stats?.duplicates_resolved} label="Duplicates Resolved" color={colors.accent3} />
        <StatCard value={`${c.summary_stats?.avg_confidence_pct}%`} label="Avg Confidence"  color={colors.warning} />
      </div>

      <div style={styles.grid2}>
        {/* Golden by Party Type */}
        <Panel title="▸ Golden Records by Party Type">
          <ResponsiveContainer width="100%" height={220}>
            <PieChart>
              <Pie data={c.golden_by_type || []} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={80} label={({ name, percent }) => `${name} ${(percent*100).toFixed(0)}%`} labelLine={false}>
                {(c.golden_by_type || []).map((_, i) => (
                  <Cell key={i} fill={CHART_COLORS[i % CHART_COLORS.length]} />
                ))}
              </Pie>
              <Tooltip contentStyle={{ background: colors.panel, border: `1px solid ${colors.border}`, borderRadius: "4px", fontSize: "12px" }} />
            </PieChart>
          </ResponsiveContainer>
        </Panel>

        {/* Source by System */}
        <Panel title="▸ Source Records by System">
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={c.source_by_system || []} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
              <XAxis dataKey="name" tick={{ fill: colors.textDim, fontSize: 11 }} axisLine={false} tickLine={false} />
              <YAxis tick={{ fill: colors.textDim, fontSize: 11 }} axisLine={false} tickLine={false} />
              <Tooltip contentStyle={{ background: colors.panel, border: `1px solid ${colors.border}`, borderRadius: "4px", fontSize: "12px" }} />
              <Bar dataKey="value" fill={colors.accent} radius={[3,3,0,0]} />
            </BarChart>
          </ResponsiveContainer>
        </Panel>

        {/* Quality Pass Rates */}
        <Panel title="▸ Data Quality Pass Rates (%)">
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={c.quality_pass_rates || []} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
              <XAxis dataKey="suite" tick={{ fill: colors.textDim, fontSize: 10 }} axisLine={false} tickLine={false} />
              <YAxis domain={[0, 100]} tick={{ fill: colors.textDim, fontSize: 11 }} axisLine={false} tickLine={false} />
              <Tooltip contentStyle={{ background: colors.panel, border: `1px solid ${colors.border}`, borderRadius: "4px", fontSize: "12px" }} />
              <Bar dataKey="pass_rate" fill={colors.accent2} radius={[3,3,0,0]} />
            </BarChart>
          </ResponsiveContainer>
        </Panel>

        {/* Lineage Events */}
        <Panel title="▸ Lineage Events by Type">
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={c.lineage_events || []} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
              <XAxis dataKey="name" tick={{ fill: colors.textDim, fontSize: 10 }} axisLine={false} tickLine={false} />
              <YAxis tick={{ fill: colors.textDim, fontSize: 11 }} axisLine={false} tickLine={false} />
              <Tooltip contentStyle={{ background: colors.panel, border: `1px solid ${colors.border}`, borderRadius: "4px", fontSize: "12px" }} />
              <Bar dataKey="value" fill={colors.accent3} radius={[3,3,0,0]} />
            </BarChart>
          </ResponsiveContainer>
        </Panel>
      </div>
    </>
  );
}

// ============================================================
// MAIN APP
// ============================================================
export default function App() {
  const [lastRefresh, setLastRefresh]     = useState(new Date());
  const [triggerStatus, setTriggerStatus] = useState({});
  const [notification, setNotification]   = useState(null);

  const { data: pipelineStatus, refetch: refetchPipeline } =
    useAPI("/pipeline/status", REFRESH_INTERVAL);

  const { data: postgresStats, refetch: refetchPostgres } =
    useAPI("/system/postgres/stats", REFRESH_INTERVAL);

  const { data: s3Files, refetch: refetchS3 } =
    useAPI("/system/s3/files", REFRESH_INTERVAL);

  const { data: kafkaStats, refetch: refetchKafka } =
    useAPI("/system/kafka/stats", 10000);

  const { data: chartData, refetch: refetchCharts } =
    useAPI("/dashboard/charts", REFRESH_INTERVAL);

  const refetchAll = useCallback(() => {
    refetchPipeline();
    refetchPostgres();
    refetchS3();
    refetchKafka();
    refetchCharts();
    setLastRefresh(new Date());
  }, [refetchPipeline, refetchPostgres, refetchS3, refetchKafka, refetchCharts]);

  const handleTrigger = async (step) => {
    setTriggerStatus(prev => ({ ...prev, [step]: "running" }));
    setNotification({ type: "info", msg: `▸ Triggering ${STEP_LABELS[step]}...` });

    try {
      const res  = await fetch(`${API_BASE}/pipeline/trigger/${step}`, { method: "POST" });
      const data = await res.json();

      // Poll for completion
      const poll = setInterval(async () => {
        const statusRes  = await fetch(`${API_BASE}/pipeline/trigger/${step}/status`);
        const statusData = await statusRes.json();

        if (statusData.status === "success") {
          clearInterval(poll);
          setTriggerStatus(prev => ({ ...prev, [step]: "success" }));
          setNotification({ type: "success", msg: `✓ ${STEP_LABELS[step]} completed successfully` });
          refetchAll();
        } else if (statusData.status === "error") {
          clearInterval(poll);
          setTriggerStatus(prev => ({ ...prev, [step]: "error" }));
          setNotification({ type: "error", msg: `✗ ${STEP_LABELS[step]} failed` });
        }
      }, 3000);

      // Clear poll after 5 minutes max
      setTimeout(() => clearInterval(poll), 300000);

    } catch (e) {
      setTriggerStatus(prev => ({ ...prev, [step]: "error" }));
      setNotification({ type: "error", msg: `✗ Failed to trigger ${step}: ${e.message}` });
    }

    // Clear notification after 5 seconds
    setTimeout(() => setNotification(null), 5000);
  };

  return (
    <div style={styles.app}>
      {/* Google Font */}
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600;700&display=swap');
        * { box-sizing: border-box; margin: 0; padding: 0; }
        ::-webkit-scrollbar { width: 4px; height: 4px; }
        ::-webkit-scrollbar-track { background: ${colors.bg}; }
        ::-webkit-scrollbar-thumb { background: ${colors.muted}; border-radius: 2px; }
        body { background: ${colors.bg}; }
        @keyframes pulse { 0%,100% { opacity:1 } 50% { opacity:0.4 } }
      `}</style>

      {/* Header */}
      <div style={styles.header}>
        <div style={styles.headerLeft}>
          <div style={styles.logo}>◈</div>
          <div>
            <div style={styles.headerTitle}>Party Master Control Center</div>
            <div style={styles.headerSub}>
              LPL Financial — Reference Data Hub
            </div>
          </div>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: "12px" }}>
          {notification && (
            <div style={{
              padding:      "6px 14px",
              borderRadius: "3px",
              fontSize:     "12px",
              background:   notification.type === "success" ? `${colors.success}15` :
                            notification.type === "error"   ? `${colors.error}15`   :
                            `${colors.accent}15`,
              border:       `1px solid ${
                            notification.type === "success" ? colors.success :
                            notification.type === "error"   ? colors.error   :
                            colors.accent}`,
              color:        notification.type === "success" ? colors.success :
                            notification.type === "error"   ? colors.error   :
                            colors.accent,
            }}>
              {notification.msg}
            </div>
          )}
          <div style={{ fontSize: "11px", color: colors.textDim }}>
            Last refresh: {lastRefresh.toLocaleTimeString()}
          </div>
          <button style={styles.btn()} onClick={refetchAll}>
            ↻ Refresh
          </button>
          <span style={styles.badge}>● LIVE</span>
        </div>
      </div>

      {/* Main Content */}
      <div style={styles.main}>

        {/* Pipeline Flow — Full Width */}
        <PipelineFlow
          status={pipelineStatus}
          onTrigger={handleTrigger}
          triggerStatus={triggerStatus}
        />

        {/* Charts */}
        <ChartsPanel charts={chartData} />

        {/* System Panels */}
        <div style={styles.grid3}>
          <PostgresPanel stats={postgresStats} />
          <S3Panel files={s3Files} />
          <KafkaPanel kafka={kafkaStats} />
        </div>

      </div>
    </div>
  );
}
