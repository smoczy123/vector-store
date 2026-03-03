/* ───────────────────────────────────────────────────────────
   ScyllaDB VSS Sizing Calculator — Frontend Logic
   ─────────────────────────────────────────────────────────── */

(function () {
  "use strict";

  // ── Helpers ──────────────────────────────────────────────

  /** Format a number with thousands separators. */
  function fmt(n) {
    return Number(n).toLocaleString("en-US");
  }

  /** Format bytes to a human-readable string. */
  function fmtBytes(bytes) {
    if (bytes < 1024) return bytes + " B";
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + " KiB";
    if (bytes < 1024 * 1024 * 1024)
      return (bytes / (1024 * 1024)).toFixed(1) + " MiB";
    return (bytes / (1024 * 1024 * 1024)).toFixed(2) + " GiB";
  }

  /** Parse a human-readable byte string back to a number of bytes. */
  function parseBytes(str) {
    str = str.trim();
    var m = str.match(/^([0-9.,]+)\s*(B|KiB|KB|MiB|MB|GiB|GB)?$/i);
    if (!m) return NaN;
    var num = parseFloat(m[1].replace(/,/g, ""));
    if (isNaN(num)) return NaN;
    var unit = (m[2] || "B").toLowerCase();
    switch (unit) {
      case "b":   return num;
      case "kib": case "kb": return num * 1024;
      case "mib": case "mb": return num * 1024 * 1024;
      case "gib": case "gb": return num * 1024 * 1024 * 1024;
      default: return num;
    }
  }

  /** Parse a formatted number string (strip commas, whitespace). */
  function parseNum(str) {
    return parseFloat(String(str).replace(/[,%\s]/g, ""));
  }

  /** Convert a log10 slider value to the actual integer. */
  function logToValue(logVal) {
    return Math.round(Math.pow(10, parseFloat(logVal)));
  }

  /** Convert an actual integer value to log10 for the slider. */
  function valueToLog(val) {
    return Math.log10(Math.max(val, 1));
  }

  /** Return a debounced version of *fn* that waits *ms* after the last call. */
  function debounce(fn, ms) {
    var timer;
    return function () {
      clearTimeout(timer);
      timer = setTimeout(fn, ms);
    };
  }

  // ── DOM References ──────────────────────────────────────

  const elNumVectors       = document.getElementById("num_vectors");
  const elNumVectorsVal    = document.getElementById("num_vectors_val");
  const elDimensions       = document.getElementById("dimensions");
  const elDimensionsVal    = document.getElementById("dimensions_val");
  const elTargetQps        = document.getElementById("target_qps");
  const elTargetQpsVal     = document.getElementById("target_qps_val");
  const elRecall           = document.getElementById("recall");
  const elRecallVal        = document.getElementById("recall_val");
  const elK               = document.getElementById("k");
  const elKVal             = document.getElementById("k_val");
  const elMetadataBytes    = document.getElementById("metadata_bytes");
  const elMetadataBytesVal = document.getElementById("metadata_bytes_val");
  const elFilteringCols    = document.getElementById("filtering_columns");
  const elFilteringColsVal = document.getElementById("filtering_columns_val");
  const elResultsContent   = document.getElementById("results-content");
  const elErrorBox         = document.getElementById("error-box");

  // ── Exact-value overrides for log-scale fields ──────────
  // When the user types an exact number the slider cannot represent
  // precisely (due to step quantisation), we store the real value here
  // so that collectInput() and the display use it instead of
  // round-tripping through the slider.
  var overrides = {
    num_vectors: null,      // integer or null
    metadata_bytes: null,   // integer (bytes) or null
  };

  // ── Value display updaters ──────────────────────────────

  /** Auto-size a text input to fit its content. */
  function autoSize(input) {
    // Use a minimum of 3ch and add 1ch padding
    var len = Math.max(input.value.length, 3);
    input.style.width = (len + 1) + "ch";
  }

  function updateDisplays() {
    elNumVectorsVal.value    = fmt(overrides.num_vectors != null
                                   ? overrides.num_vectors
                                   : logToValue(elNumVectors.value));
    elDimensionsVal.value    = fmt(elDimensions.value);
    elTargetQpsVal.value     = fmt(elTargetQps.value);
    elRecallVal.value        = elRecall.value + " %";
    elKVal.value             = fmt(elK.value);
    elMetadataBytesVal.value = fmtBytes(overrides.metadata_bytes != null
                                         ? overrides.metadata_bytes
                                         : logToValue(elMetadataBytes.value));
    elFilteringColsVal.value = elFilteringCols.value;

    // Auto-size all value inputs.
    [elNumVectorsVal, elDimensionsVal, elTargetQpsVal, elRecallVal, elKVal,
     elMetadataBytesVal, elFilteringColsVal].forEach(autoSize);

    // Highlight active dimension chip.
    document.querySelectorAll('.chip[data-target="dimensions"]').forEach(function (chip) {
      chip.classList.toggle("active", chip.dataset.value === elDimensions.value);
    });
  }

  // Debounced compute triggered on any input change.
  var scheduleCompute = debounce(compute, 150);

  function onInputChange() {
    updateDisplays();
    scheduleCompute();
  }

  // Wire up real-time display updates + auto-compute.
  // Moving a slider manually clears any exact-value override for that field.
  [elNumVectors, elDimensions, elTargetQps, elRecall, elK, elMetadataBytes, elFilteringCols]
    .forEach(function (el) {
      el.addEventListener("input", function () {
        if (el === elNumVectors)   overrides.num_vectors = null;
        if (el === elMetadataBytes) overrides.metadata_bytes = null;
        onInputChange();
      });
    });

  // ── Text input → slider sync ────────────────────────────

  /** Clamp a value between min and max. */
  function clamp(val, min, max) {
    return Math.min(Math.max(val, min), max);
  }

  /**
   * Wire a text input so that on Enter or blur, the typed value is parsed
   * and pushed back into the corresponding slider.  *parseFn* converts the
   * raw text to the slider's native value (which may be log-scale).
   */
  function wireTextInput(textEl, sliderEl, parseFn) {
    function commit() {
      var parsed = parseFn(textEl.value);
      if (isNaN(parsed)) {
        // Revert to current slider value.
        updateDisplays();
        return;
      }
      var lo = parseFloat(sliderEl.min);
      var hi = parseFloat(sliderEl.max);
      sliderEl.value = clamp(parsed, lo, hi);
      onInputChange();
    }

    textEl.addEventListener("keydown", function (e) {
      if (e.key === "Enter") {
        e.preventDefault();
        textEl.blur();
      }
    });
    textEl.addEventListener("blur", commit);
  }

  // num_vectors — log-scale with exact override.
  wireTextInput(elNumVectorsVal, elNumVectors, function (str) {
    var n = Math.round(parseNum(str));
    if (isNaN(n) || n < 1) return NaN;
    overrides.num_vectors = n;
    return valueToLog(n);
  });

  // dimensions — linear.
  wireTextInput(elDimensionsVal, elDimensions, parseNum);

  // target_qps — linear.
  wireTextInput(elTargetQpsVal, elTargetQps, parseNum);

  // recall — strip "%" suffix.
  wireTextInput(elRecallVal, elRecall, function (str) {
    return parseNum(str.replace(/%/g, ""));
  });

  // k — linear.
  wireTextInput(elKVal, elK, parseNum);

  // metadata_bytes — log-scale with exact override.
  wireTextInput(elMetadataBytesVal, elMetadataBytes, function (str) {
    var bytes = parseBytes(str);
    if (isNaN(bytes)) bytes = parseNum(str);
    if (isNaN(bytes) || bytes < 1) return NaN;
    bytes = Math.round(bytes);
    overrides.metadata_bytes = bytes;
    return valueToLog(bytes);
  });

  // filtering_columns — linear.
  wireTextInput(elFilteringColsVal, elFilteringCols, parseNum);

  // ── Preset chips ────────────────────────────────────────

  document.querySelectorAll(".chip").forEach(function (chip) {
    chip.addEventListener("click", function () {
      var target = document.getElementById(chip.dataset.target);
      if (target) {
        target.value = chip.dataset.value;
        target.dispatchEvent(new Event("input"));
      }
    });
  });

  // ── Quantization radio cards ────────────────────────────

  document.querySelectorAll(".radio-card").forEach(function (card) {
    card.addEventListener("click", function () {
      // Only affect radio cards within the same group.
      var group = card.closest(".radio-group");
      group.querySelectorAll(".radio-card").forEach(function (c) {
        c.classList.remove("selected");
      });
      card.classList.add("selected");
      card.querySelector("input").checked = true;
      scheduleCompute();
    });
  });

  // ── Collect input values ────────────────────────────────

  function collectInput() {
    var quantEl = document.querySelector('input[name="quantization"]:checked');
    var cloudEl = document.querySelector('input[name="cloud_provider"]:checked');
    return {
      num_vectors:            overrides.num_vectors != null
                                ? overrides.num_vectors
                                : logToValue(elNumVectors.value),
      dimensions:             parseInt(elDimensions.value, 10),
      target_qps:             parseInt(elTargetQps.value, 10),
      recall:                 parseInt(elRecall.value, 10),
      k:                      parseInt(elK.value, 10),
      quantization:           quantEl ? quantEl.value : "none",
      metadata_bytes_per_vector: overrides.metadata_bytes != null
                                  ? overrides.metadata_bytes
                                  : logToValue(elMetadataBytes.value),
      filtering_columns:      parseInt(elFilteringCols.value, 10),
      cloud_provider:         cloudEl ? cloudEl.value : "aws",
    };
  }

  // ── Render results ──────────────────────────────────────

  function renderResults(data) {
    var s  = data.search_node;
    var i  = data.instance_selection;
    var d  = data.data_node;
    var h  = data.hnsw;

    var html = "";

    // Cost banner
    html += '<div class="cost-banner">';
    html += '  <div class="small-label">Estimated Search-Node Cost</div>';
    html += '  <div class="big-number">$' + fmt(i.total_cost_per_month) + " / mo</div>";
    html += '  <div class="small-label">$' + i.total_cost_per_hour.toFixed(3) + " / hr</div>";
    html += "</div>";

    // Instance Selection
    html += '<div class="result-card">';
    html += '  <div class="result-section-title">Search Node Instances</div>';
    html += resultRow("Instance type",       i.instance_type);
    html += resultRow("Instances",           i.num_instances);
    html += resultRow("vCPUs per instance",  i.instance_vcpus);
    html += resultRow("RAM per instance",    i.instance_ram_gb + " GB");
    html += resultRow("Total vCPUs",         fmt(i.total_vcpus));
    html += resultRow("Total RAM",           fmt(i.total_ram_gb) + " GB");
    html += "</div>";

    // Search Node Sizing
    html += '<div class="result-card">';
    html += '  <div class="result-section-title">Search Node Sizing (per replica)</div>';
    html += resultRow("Index RAM",           s.index_ram_gb.toFixed(2) + " GB");
    html += resultRow("Filtering RAM",       s.filtering_ram_gb.toFixed(2) + " GB");
    html += resultRow("Total RAM required",  s.total_ram_gb.toFixed(2) + " GB", true);
    html += resultRow("Required vCPUs",      fmt(s.required_vcpus), true);
    html += resultRow("Throughput bucket",   s.throughput_bucket);
    html += resultRow("Base QPS/vCPU",       s.base_qps_per_vcpu.toFixed(1));
    html += resultRow("Effective QPS/vCPU",  s.effective_qps_per_vcpu.toFixed(1));
    html += "</div>";

    // HNSW Parameters
    html += '<div class="result-card">';
    html += '  <div class="result-section-title">HNSW Parameters</div>';
    html += resultRow("M (connectivity)",    h.m);
    html += resultRow("Compression ratio",   data.compression_ratio + "×");
    html += "</div>";

    // Data Nodes
    html += '<div class="result-card">';
    html += '  <div class="result-section-title">ScyllaDB Data Nodes</div>';
    html += resultRow("Nodes (RF=3)",        d.num_nodes);
    html += resultRow("vCPUs per node",      d.vcpus_per_node);
    html += resultRow("Total vCPUs",         fmt(d.total_vcpus));
    html += resultRow("Embedding storage",   d.embedding_storage_gb.toFixed(2) + " GB");
    html += resultRow("Metadata storage",    d.metadata_storage_gb.toFixed(2) + " GB");
    html += resultRow("Total storage",       d.total_storage_gb.toFixed(2) + " GB", true);
    html += "</div>";

    elResultsContent.innerHTML = html;
  }

  function resultRow(label, value, highlight) {
    var cls = highlight ? ' highlight' : '';
    return '<div class="result-row">' +
      '<span class="result-label">' + label + '</span>' +
      '<span class="result-value' + cls + '">' + value + '</span>' +
      '</div>';
  }

  // ── API call ────────────────────────────────────────────

  function compute() {

    elErrorBox.classList.add("hidden");

    fetch("/api/compute", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(collectInput()),
    })
      .then(function (resp) { return resp.json().then(function (d) { return { ok: resp.ok, data: d }; }); })
      .then(function (result) {
        if (!result.ok) {
          elErrorBox.textContent = result.data.error || "Unknown error";
          elErrorBox.classList.remove("hidden");
          return;
        }
        renderResults(result.data);
      })
      .catch(function (err) {
        elErrorBox.textContent = "Request failed: " + err.message;
        elErrorBox.classList.remove("hidden");
      });
  }

  // ── Shareable URL ───────────────────────────────────────

  /** Build a URL with all current input values as query parameters. */
  function buildShareUrl() {
    var input = collectInput();
    var params = new URLSearchParams();
    params.set("num_vectors",      input.num_vectors);
    params.set("dimensions",       input.dimensions);
    params.set("target_qps",       input.target_qps);
    params.set("recall",           input.recall);
    params.set("k",                input.k);
    params.set("quantization",     input.quantization);
    params.set("metadata_bytes",   input.metadata_bytes_per_vector);
    params.set("filtering_columns", input.filtering_columns);
    params.set("cloud_provider",    input.cloud_provider);
    return window.location.origin + window.location.pathname + "?" + params.toString();
  }

  /** Restore inputs from URL query parameters (if present). */
  function restoreFromUrl() {
    var params = new URLSearchParams(window.location.search);
    if (params.toString() === "") return;

    var v;

    v = parseInt(params.get("num_vectors"), 10);
    if (!isNaN(v) && v >= 1) {
      overrides.num_vectors = v;
      elNumVectors.value = clamp(valueToLog(v),
        parseFloat(elNumVectors.min), parseFloat(elNumVectors.max));
    }

    v = parseInt(params.get("dimensions"), 10);
    if (!isNaN(v)) elDimensions.value = clamp(v,
      parseInt(elDimensions.min, 10), parseInt(elDimensions.max, 10));

    v = parseInt(params.get("target_qps"), 10);
    if (!isNaN(v)) elTargetQps.value = clamp(v,
      parseInt(elTargetQps.min, 10), parseInt(elTargetQps.max, 10));

    v = parseInt(params.get("recall"), 10);
    if (!isNaN(v)) elRecall.value = clamp(v,
      parseInt(elRecall.min, 10), parseInt(elRecall.max, 10));

    v = parseInt(params.get("k"), 10);
    if (!isNaN(v)) elK.value = clamp(v,
      parseInt(elK.min, 10), parseInt(elK.max, 10));

    var q = params.get("quantization");
    if (q) {
      var radio = document.querySelector('input[name="quantization"][value="' + q + '"]');
      if (radio) {
        document.querySelectorAll(".radio-card").forEach(function (c) {
          c.classList.remove("selected");
        });
        radio.checked = true;
        radio.closest(".radio-card").classList.add("selected");
      }
    }

    v = parseInt(params.get("metadata_bytes"), 10);
    if (!isNaN(v) && v >= 1) {
      overrides.metadata_bytes = v;
      elMetadataBytes.value = clamp(valueToLog(v),
        parseFloat(elMetadataBytes.min), parseFloat(elMetadataBytes.max));
    }

    v = parseInt(params.get("filtering_columns"), 10);
    if (!isNaN(v)) elFilteringCols.value = clamp(v,
      parseInt(elFilteringCols.min, 10), parseInt(elFilteringCols.max, 10));

    var cp = params.get("cloud_provider");
    if (cp) {
      var cpRadio = document.querySelector('input[name="cloud_provider"][value="' + cp + '"]');
      if (cpRadio) {
        document.querySelectorAll('.cloud-provider-group .radio-card').forEach(function (c) {
          c.classList.remove("selected");
        });
        cpRadio.checked = true;
        cpRadio.closest(".radio-card").classList.add("selected");
      }
    }
  }

  // Copy Link button.
  var elCopyBtn   = document.getElementById("copy-link-btn");
  var elCopyLabel = document.getElementById("copy-link-label");

  if (elCopyBtn) {
    elCopyBtn.addEventListener("click", function () {
      var url = buildShareUrl();
      navigator.clipboard.writeText(url).then(function () {
        elCopyLabel.textContent = "Copied!";
        elCopyBtn.classList.add("copied");
        setTimeout(function () {
          elCopyLabel.textContent = "Copy Link";
          elCopyBtn.classList.remove("copied");
        }, 1500);
      });
    });
  }

  // ── Initialise ──────────────────────────────────────────
  restoreFromUrl();
  updateDisplays();

  // Auto-compute on load with defaults.
  compute();
})();
