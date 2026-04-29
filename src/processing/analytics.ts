type Row = Record<string, unknown>;

function resolveProduction(row: Row): number {
  for (const field of ["NetProduction", "Collected", "ProjectedTreatmentPlanNetProduction"]) {
    const raw = row[field];
    if (raw !== undefined && raw !== null) {
      const v = parseFloat(String(raw));
      if (!isNaN(v) && v !== 0) return v;
    }
  }
  return 100;
}

function parseDate(row: Row): string {
  const ts = String(row["HistDateTStamp"] ?? "");
  if (!ts) return "Unknown";
  try {
    const d = new Date(ts.slice(0, 19).replace(" ", "T") + "Z");
    if (isNaN(d.getTime())) return "Unknown";
    return d.toISOString().slice(0, 10);
  } catch {
    return "Unknown";
  }
}

function dateLabel(ymd: string): string {
  try {
    const d = new Date(ymd + "T12:00:00Z");
    return d.toLocaleDateString("en-US", { month: "long", day: "2-digit", year: "numeric", timeZone: "UTC" });
  } catch {
    return ymd;
  }
}

function fmt(v: number): string {
  return `$${v.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function isHygiene(row: Row): boolean {
  return String(row["IsHygiene"] ?? "0").trim() === "1";
}
function isReschedule(row: Row): boolean {
  return String(row["AptType"] ?? "").includes("Reschedule");
}
function isComplete(row: Row): boolean {
  return String(row["AppointmentStatus"] ?? "").trim() === "Complete";
}

export function buildTableA(records: Row[]) {
  const daily = new Map<string, { hygiene: number; restorative: number; reschedule: number; new_appt: number; completed: number; other: number }>();
  for (const row of records) {
    const ymd = parseDate(row);
    if (!daily.has(ymd)) daily.set(ymd, { hygiene: 0, restorative: 0, reschedule: 0, new_appt: 0, completed: 0, other: 0 });
    const d = daily.get(ymd)!;
    const prod = resolveProduction(row);
    if (isHygiene(row)) d.hygiene += prod; else d.restorative += prod;
    if (isReschedule(row)) d.reschedule++; else d.new_appt++;
    if (isComplete(row)) d.completed++; else d.other++;
  }
  const rows = [];
  const sorted = [...daily.keys()].filter(k => k !== "Unknown").sort();
  if (daily.has("Unknown")) sorted.push("Unknown");
  for (const ymd of sorted) {
    const d = daily.get(ymd)!;
    const total = d.hygiene + d.restorative;
    rows.push({ date: ymd === "Unknown" ? "Unknown" : dateLabel(ymd), hygiene: fmt(d.hygiene), restorative: fmt(d.restorative), total: fmt(total), reschedule_count: d.reschedule, new_appt_count: d.new_appt, completed: d.completed, other: d.other });
  }
  return rows;
}

export function buildTableB(records: Row[]) {
  const cats = new Map<string, { production: number; reschedule: number; new_appt: number; completed: number; other: number }>();
  for (const row of records) {
    const raw = String(row["ProcedureCategories"] ?? "Uncategorized").trim();
    const catList = raw.split(",").map(c => c.trim()).filter(Boolean);
    const list = catList.length ? catList : ["Uncategorized"];
    const share = resolveProduction(row) / list.length;
    for (const cat of list) {
      if (!cats.has(cat)) cats.set(cat, { production: 0, reschedule: 0, new_appt: 0, completed: 0, other: 0 });
      const d = cats.get(cat)!;
      d.production += share;
      if (isReschedule(row)) d.reschedule++; else d.new_appt++;
      if (isComplete(row)) d.completed++; else d.other++;
    }
  }
  return [...cats.entries()]
    .map(([cat, d]) => ({ category: cat, production: fmt(d.production), reschedule_count: d.reschedule, new_appt_count: d.new_appt, completed: d.completed, other: d.other }))
    .sort((a, b) => parseFloat(b.production.replace(/[$,]/g, "")) - parseFloat(a.production.replace(/[$,]/g, "")));
}

export function buildTableC(records: Row[]) {
  const daily = new Map<string, { new_prod: number; reschedule_prod: number; new_count: number; reschedule_count: number; new_completed: number; reschedule_completed: number }>();
  for (const row of records) {
    const ymd = parseDate(row);
    if (!daily.has(ymd)) daily.set(ymd, { new_prod: 0, reschedule_prod: 0, new_count: 0, reschedule_count: 0, new_completed: 0, reschedule_completed: 0 });
    const d = daily.get(ymd)!;
    const prod = resolveProduction(row);
    if (isReschedule(row)) {
      d.reschedule_prod += prod; d.reschedule_count++;
      if (isComplete(row)) d.reschedule_completed++;
    } else {
      d.new_prod += prod; d.new_count++;
      if (isComplete(row)) d.new_completed++;
    }
  }
  const rows = [];
  const sorted = [...daily.keys()].filter(k => k !== "Unknown").sort();
  if (daily.has("Unknown")) sorted.push("Unknown");
  for (const ymd of sorted) {
    const d = daily.get(ymd)!;
    rows.push({ date: ymd === "Unknown" ? "Unknown" : dateLabel(ymd), new_prod: fmt(d.new_prod), reschedule_prod: fmt(d.reschedule_prod), new_count: d.new_count, reschedule_count: d.reschedule_count, new_completed: d.new_completed, reschedule_completed: d.reschedule_completed });
  }
  return rows;
}

export function buildAnalytics(records: Row[]) {
  if (!records.length) return { table_a: [], table_b: [], table_c: [], total_records: 0 };
  return { table_a: buildTableA(records), table_b: buildTableB(records), table_c: buildTableC(records), total_records: records.length };
}
