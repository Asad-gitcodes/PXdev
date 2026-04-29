import { settings } from "../config.js";

const QUERY = `
SELECT
  a.IsNewPatient, a.PatNum, ha.AptNum, ha.HistDateTStamp, ha.Note,
  CONCAT(p.LName, ' ', p.FName) AS PatientName,
  p.WirelessPhone, p.Email, p.SecDateEntry AS PatientSet,
  COALESCE(SUM(pl.ProcFee*(pl.UnitQty+pl.BaseUnits))+SUM(COALESCE(ad.AdjAmt,0))-SUM(COALESCE(cp.WriteOff,0)),0) AS NetProduction,
  a.AptDateTime,
  CASE WHEN a.AptStatus=1 THEN 'Scheduled' WHEN a.AptStatus=2 THEN 'Complete'
       WHEN a.AptStatus=3 THEN 'Broken'    WHEN a.AptStatus=4 THEN 'ASAP'
       WHEN a.AptStatus=5 THEN 'Unscheduled' ELSE 'Unknown' END AS AppointmentStatus,
  (SELECT MIN(a2.AptDateTime) FROM appointment a2 WHERE a2.PatNum=a.PatNum AND a2.AptStatus=2) AS FirstCompletedApptDate,
  CASE WHEN a.AptDateTime=(SELECT MIN(a2.AptDateTime) FROM appointment a2 WHERE a2.PatNum=a.PatNum AND a2.AptStatus=2)
       THEN 'New Patient' ELSE 'Existing Patient' END AS PatientFlag,
  CASE WHEN ha.Note LIKE '%Rescheduled by PX VoiceAI%' THEN 'Voice AI Reschedule'
       ELSE 'Voice AI New Appointment' END AS AptType,
  a.IsHygiene,
  GROUP_CONCAT(DISTINCT def.ItemName) AS ProcedureCategories,
  (SELECT COALESCE(SUM(pt.PayAmt),0) FROM payment pt WHERE pt.PatNum=p.PatNum) AS Collected,
  (SELECT COALESCE(SUM(pl2.ProcFee*(pl2.UnitQty+pl2.BaseUnits))-SUM(COALESCE(cp2.WriteOffEst,0)),0)
   FROM procedurelog pl2 LEFT JOIN claimproc cp2 ON cp2.ProcNum=pl2.ProcNum
   WHERE pl2.PatNum=a.PatNum AND pl2.ProcStatus=1) AS ProjectedTreatmentPlanNetProduction
FROM histappointment ha
INNER JOIN appointment a   ON ha.AptNum=a.AptNum
INNER JOIN patient     p   ON p.PatNum=a.PatNum
LEFT  JOIN procedurelog pl ON a.AptNum=pl.AptNum AND pl.ProcStatus IN (1,2)
LEFT  JOIN adjustment  ad  ON ad.PatNum=pl.PatNum
LEFT  JOIN claimproc   cp  ON cp.ProcNum=pl.ProcNum
LEFT  JOIN procedurecode prc ON prc.CodeNum=pl.CodeNum
LEFT  JOIN definition  def ON def.DefNum=prc.ProcCat
WHERE ha.Note LIKE '%PX Voice%' AND ha.HistApptAction=0
  AND DATE(ha.HistDateTStamp)='{report_date}'
GROUP BY ha.AptNum ORDER BY 4 DESC LIMIT 1500
`.trim();

export async function getAppointments(licenseKey: string, reportDate: string): Promise<Record<string, unknown>[]> {
  try {
    const query = QUERY.replace("{report_date}", reportDate);
    const res = await fetch(settings.queryApiUrl, {
      method: "POST",
      headers: { Authorization: settings.queryApiToken, "Content-Type": "application/json" },
      body: JSON.stringify({ key: licenseKey, query }),
      signal: AbortSignal.timeout(30_000),
    });
    if (!res.ok) return [];
    const raw = await res.json();
    if (Array.isArray(raw)) return raw;
    return raw.data ?? raw.results ?? raw.rows ?? [];
  } catch {
    return [];
  }
}
