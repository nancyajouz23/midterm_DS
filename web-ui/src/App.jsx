
import React, { useState } from "react";
import {
  Database, Server, Search, Upload, Download,
  Trash2, RefreshCw, ShieldCheck, HardDrive,
  FolderPlus, File, Activity
} from "lucide-react";

export default function App() {
  const [apiKey, setApiKey] = useState("");
  const [connected, setConnected] = useState(false);
  const [tab, setTab] = useState("objects");
  const [buckets, setBuckets] = useState([]);
  const [bucket, setBucket] = useState("");
  const [newBucket, setNewBucket] = useState("");
  const [mode, setMode] = useState("replication");
  const [file, setFile] = useState(null);
  const [objects, setObjects] = useState([]);
  const [cluster, setCluster] = useState(null);
  const [prefix, setPrefix] = useState("");
  const [minSize, setMinSize] = useState("");
  const [tagKey, setTagKey] = useState("");
  const [tagValue, setTagValue] = useState("");
  const [results, setResults] = useState([]);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");

  async function api(path, options = {}) {
    const response = await fetch(`/api${path}`, {
      ...options,
      headers: {
        "X-API-Key": apiKey,
        ...(options.headers || {})
      }
    });

    if (!response.ok) {
      const detail = await response.text();
      throw new Error(`HTTP ${response.status}: ${detail.slice(0, 300)}`);
    }

    return response.json();
  }

  async function execute(action) {
    setBusy(true);
    setError("");
    setMessage("");
    try {
      await action();
    } catch (e) {
      setError(e.message);
    } finally {
      setBusy(false);
    }
  }

  async function refreshBuckets() {
    const data = await api("/buckets");
    setBuckets(data);
    if (!bucket && data.length) setBucket(data[0].name);
  }

  async function refreshObjects(selected = bucket) {
    if (!selected) {
      setObjects([]);
      return;
    }

    const data = await api(
      `/objects?bucket=${encodeURIComponent(selected)}`
    );
    setObjects(data);
  }

  async function refreshCluster() {
    const data = await api("/cluster/status");
    setCluster(data);
  }

  async function connect() {
    await execute(async () => {
      await Promise.all([refreshBuckets(), refreshCluster()]);
      setConnected(true);
      setMessage("Connected to the storage gateway.");
    });
  }

  async function createBucket(e) {
    e.preventDefault();
    await execute(async () => {
      await api("/buckets", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({name: newBucket, mode})
      });
      await refreshBuckets();
      setBucket(newBucket);
      setNewBucket("");
      setMessage("Bucket created successfully.");
      await refreshObjects(newBucket);
    });
  }

  async function upload(e) {
    e.preventDefault();
    if (!file || !bucket) return;

    await execute(async () => {
      const path = `/objects/${encodeURIComponent(bucket)}/` +
                   encodeURIComponent(file.name);

      const response = await api(path, {
        method: "PUT",
        headers: {"Content-Type": "application/octet-stream"},
        body: file
      });

      setMessage(
        `Uploaded ${response.key} (${response.chunks} chunks, ` +
        `${response.mode}).`
      );
      setFile(null);
      document.getElementById("file-input").value = "";
      await refreshObjects();
      await refreshCluster();
    });
  }

  async function download(obj) {
    await execute(async () => {
      const path = `/api/objects/${encodeURIComponent(obj.bucket)}/` +
                   encodeURIComponent(obj.object_key);
      const response = await fetch(path, {
        headers: {"X-API-Key": apiKey}
      });

      if (!response.ok) {
        throw new Error(`Download failed: HTTP ${response.status}`);
      }

      const blob = await response.blob();
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = obj.object_key.split("/").pop();
      document.body.appendChild(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(url);
      setMessage("Download completed.");
    });
  }

  async function remove(obj) {
    if (!window.confirm(`Delete ${obj.object_key}?`)) return;

    await execute(async () => {
      await api(
        `/objects/${encodeURIComponent(obj.bucket)}/` +
        encodeURIComponent(obj.object_key) +
        `?version=${obj.version}`,
        {method: "DELETE"}
      );
      await refreshObjects();
      setMessage("Object version deleted.");
    });
  }

  async function search(e) {
    e.preventDefault();

    await execute(async () => {
      const query = new URLSearchParams();
      if (bucket) query.set("bucket", bucket);
      if (prefix) query.set("prefix", prefix);
      if (minSize) query.set("min_size", minSize);
      if (tagKey) query.set("tag_key", tagKey);
      if (tagValue) query.set("tag_value", tagValue);

      const data = await api(
        `/search/elasticsearch?${query.toString()}`
      );
      setResults(data);
      setMessage(`${data.length} indexed object(s) found.`);
    });
  }

  function human(bytes) {
    if (bytes === undefined || bytes === null) return "—";
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1048576) return `${(bytes / 1024).toFixed(1)} KB`;
    if (bytes < 1073741824) {
      return `${(bytes / 1048576).toFixed(1)} MB`;
    }
    return `${(bytes / 1073741824).toFixed(1)} GB`;
  }

  function objectTable(items) {
    return (
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Object key</th>
              <th>Size</th>
              <th>Version</th>
              <th>Created</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {items.map((obj) => (
              <tr key={`${obj.bucket}/${obj.object_key}/${obj.version}`}>
                <td className="file-name">
                  <File size={17} />
                  <span>{obj.object_key}</span>
                </td>
                <td>{human(obj.size)}</td>
                <td>v{obj.version}</td>
                <td>{new Date(obj.created_at).toLocaleString()}</td>
                <td>
                  <div className="actions">
                    <button className="icon-button"
                      title="Download"
                      onClick={() => download(obj)}>
                      <Download size={17} />
                    </button>
                    <button className="icon-button danger"
                      title="Delete this version"
                      onClick={() => remove(obj)}>
                      <Trash2 size={17} />
                    </button>
                  </div>
                </td>
              </tr>
            ))}
            {items.length === 0 && (
              <tr>
                <td colSpan="5" className="empty">No objects found.</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    );
  }

  return (
    <div className="layout">
      <aside className="sidebar">
        <div className="brand">
          <Database size={28} />
          <div>
            <strong>Distributed Storage</strong>
            <small>Management Console</small>
          </div>
        </div>
        <nav>
          {[
            ["objects", HardDrive, "Objects & Buckets"],
            ["search", Search, "Elasticsearch"],
            ["cluster", Server, "Cluster Monitor"]
          ].map(([id, Icon, label]) => (
            <button key={id}
              className={tab === id ? "nav active" : "nav"}
              onClick={() => setTab(id)}>
              <Icon size={19} /> {label}
            </button>
          ))}
        </nav>
        <div className="sidebar-foot">
          <ShieldCheck size={17} />
          <span>SHA-256 · Quorum · Reed–Solomon</span>
        </div>
      </aside>

      <main className="main">
        <header className="topbar">
          <div>
            <p className="eyebrow">DISTRIBUTED SYSTEMS PROJECT</p>
            <h1>{
              tab === "objects" ? "Object Storage" :
              tab === "search" ? "Object Search" :
              "Cluster Monitoring"
            }</h1>
          </div>
          <span className={connected ? "status online" : "status"}>
            <span className="dot" />
            {connected ? "Connected" : "Disconnected"}
          </span>
        </header>

        <section className="card connection">
          <label>Gateway API key</label>
          <input type="password" value={apiKey}
            onChange={(e) => setApiKey(e.target.value)}
            placeholder="Enter X-API-Key" />
          <button onClick={connect} disabled={busy || !apiKey}>
            <ShieldCheck size={17} /> Connect
          </button>
        </section>

        {error && <div className="alert error">{error}</div>}
        {message && <div className="alert success">{message}</div>}

        {tab === "objects" && (
          <>
            <div className="stats">
              <div className="stat">
                <Database />
                <span>Buckets</span>
                <strong>{buckets.length}</strong>
              </div>
              <div className="stat">
                <File />
                <span>Listed objects</span>
                <strong>{objects.length}</strong>
              </div>
              <div className="stat">
                <Server />
                <span>Online nodes</span>
                <strong>{cluster?.online_nodes ?? "—"}</strong>
              </div>
            </div>

            <div className="two-col">
              <section className="card">
                <h2><FolderPlus size={20} /> Create bucket</h2>
                <form onSubmit={createBucket}>
                  <label>Bucket name</label>
                  <input required value={newBucket}
                    onChange={(e) => setNewBucket(e.target.value)}
                    placeholder="my-documents" />
                  <label>Redundancy mode</label>
                  <select value={mode}
                    onChange={(e) => setMode(e.target.value)}>
                    <option value="replication">Replication (N=3)</option>
                    <option value="erasure">Erasure coding (4+2)</option>
                  </select>
                  <button disabled={busy || !connected} type="submit">
                    Create bucket
                  </button>
                </form>
              </section>

              <section className="card">
                <h2><Upload size={20} /> Upload object</h2>
                <form onSubmit={upload}>
                  <label>Target bucket</label>
                  <select value={bucket}
                    onChange={(e) => {
                      setBucket(e.target.value);
                      refreshObjects(e.target.value)
                        .catch((err) => setError(err.message));
                    }}>
                    <option value="">Select bucket</option>
                    {buckets.map((b) => (
                      <option key={b.name} value={b.name}>
                        {b.name} — {b.mode}
                      </option>
                    ))}
                  </select>
                  <label>Choose file</label>
                  <input id="file-input" type="file" required
                    onChange={(e) => setFile(e.target.files[0])} />
                  <button type="submit"
                    disabled={busy || !file || !bucket}>
                    <Upload size={17} /> Upload file
                  </button>
                </form>
              </section>
            </div>

            <section className="card">
              <div className="section-top">
                <h2><File size={20} /> Browse objects</h2>
                <button className="secondary"
                  disabled={busy || !bucket}
                  onClick={() => execute(() => refreshObjects())}>
                  <RefreshCw size={16} /> Refresh
                </button>
              </div>
              {objectTable(objects)}
            </section>
          </>
        )}

        {tab === "search" && (
          <section className="card">
            <h2><Search size={20} /> Elasticsearch Metadata Search</h2>
            <form onSubmit={search}>
              <div className="form-grid">
                <div>
                  <label>Key prefix</label>
                  <input value={prefix}
                    onChange={(e) => setPrefix(e.target.value)}
                    placeholder="report" />
                </div>
                <div>
                  <label>Minimum size (bytes)</label>
                  <input type="number" min="0" value={minSize}
                    onChange={(e) => setMinSize(e.target.value)} />
                </div>
                <div>
                  <label>Tag key</label>
                  <input value={tagKey}
                    onChange={(e) => setTagKey(e.target.value)}
                    placeholder="category" />
                </div>
                <div>
                  <label>Tag value</label>
                  <input value={tagValue}
                    onChange={(e) => setTagValue(e.target.value)}
                    placeholder="documents" />
                </div>
              </div>
              <button disabled={busy || !connected}>
                <Search size={17} /> Search index
              </button>
            </form>
            <h3>Search results ({results.length})</h3>
            {objectTable(results)}
          </section>
        )}

        {tab === "cluster" && (
          <>
            <div className="stats">
              <div className="stat">
                <Server />
                <span>Online nodes</span>
                <strong>{cluster?.online_nodes ?? "—"}</strong>
              </div>
              <div className="stat">
                <HardDrive />
                <span>Total free space</span>
                <strong>{human(cluster?.total_free_space)}</strong>
              </div>
              <div className="stat">
                <Activity />
                <span>Degraded objects</span>
                <strong>{cluster?.degraded_objects ?? "—"}</strong>
              </div>
            </div>

            <section className="card">
              <div className="section-top">
                <h2><Server size={20} /> Storage nodes</h2>
                <button className="secondary"
                  onClick={() => execute(refreshCluster)}
                  disabled={busy || !connected}>
                  <RefreshCw size={16} /> Refresh
                </button>
              </div>
              <div className="node-grid">
                {(cluster?.nodes || []).map((node) => (
                  <div className="node" key={node.node_id}>
                    <div className="node-head">
                      <strong>{node.node_id}</strong>
                      <span className={
                        node.online ? "status online" : "status offline"
                      }>
                        {node.online ? "Online" : "Offline"}
                      </span>
                    </div>
                    <p>{node.chunk_count} chunks stored</p>
                    <div className="progress">
                      <div style={{
                        width: `${Math.min(
                          100, node.chunk_count * 2
                        )}%`
                      }} />
                    </div>
                    <small>{human(node.free_space)} free</small>
                  </div>
                ))}
              </div>
            </section>

            <section className="card">
              <h2><Activity size={20} /> Recovery status</h2>
              <p>Degraded objects: {
                cluster?.degraded_objects ?? "—"
              }</p>
              <p>Inaccessible objects: {
                cluster?.inaccessible_objects ?? "—"
              }</p>
              <p className="muted">
                Repair worker: periodic scrubbing and replica recovery.
                Detailed repair events are available in Docker logs.
              </p>
            </section>
          </>
        )}
      </main>
    </div>
  );
}
