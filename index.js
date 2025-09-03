// frontend/src/components/pages/Apiscount/index.jsx
import React, { useEffect, useState } from 'react';

// adjust the import path if your alias differs
import {
  getValuationBookCounts,
  getRiskShocksCounts,
  getSensitivitiesBookCounts,
  getSensitivityPnl,
  getValuationRunCounts,
} from '../../../api/api'; // ← change path if needed

const Box = ({ title, children }) => (
  <div style={{
    background: '#fff',
    border: '1px solid #e5e7eb',
    borderRadius: 12,
    padding: '16px 16px 8px',
    boxShadow: '0 1px 2px rgba(0,0,0,0.04)',
    marginBottom: 16
  }}>
    <div style={{ fontWeight: 600, marginBottom: 12 }}>{title}</div>
    {children}
  </div>
);

const SimpleTable = ({ rows, columns, empty }) => {
  if (!rows?.length) return <div style={{ color: '#6b7280' }}>{empty || 'No data'}</div>;
  return (
    <div style={{ overflowX: 'auto' }}>
      <table style={{ width: '100%', borderCollapse: 'collapse' }}>
        <thead>
          <tr>
            {columns.map((c) => (
              <th
                key={c.key}
                style={{
                  textAlign: 'left',
                  padding: '8px',
                  fontSize: 13,
                  color: '#6b7280',
                  borderBottom: '1px solid #e5e7eb'
                }}
              >
                {c.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i}>
              {columns.map((c) => (
                <td
                  key={c.key}
                  style={{ padding: '8px', borderBottom: '1px solid #f3f4f6' }}
                >
                  {r[c.key]}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

export default function DataStatus() {
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState('');

  const [valuationBooks, setValuationBooks] = useState([]);
  const [riskShocks, setRiskShocks] = useState([]);
  const [sensBooks, setSensBooks] = useState([]);
  const [sensPnl, setSensPnl] = useState([]);
  const [runCounts, setRunCounts] = useState(null);

  useEffect(() => {
    let cancelled = false;

    (async () => {
      try {
        setLoading(true);

        const [
          vBooks,
          rShocks,
          sBooks,
          sPnl,
          vRuns,
        ] = await Promise.all([
          getValuationBookCounts(),      // [{ book, count }]
          getRiskShocksCounts(),         // [{ risk_factor, curve, count }]
          getSensitivitiesBookCounts(),  // [{ book, count }]
          getSensitivityPnl(),           // [{ book, pnl, reval_pnl }]
          getValuationRunCounts(),       // { run_count: N }
        ]);

        if (cancelled) return;

        // If your backend uses different keys, map here before setState.
        setValuationBooks(vBooks);
        setRiskShocks(rShocks);
        setSensBooks(sBooks);
        setSensPnl(sPnl);
        setRunCounts(vRuns?.run_count ?? null);
      } catch (e) {
        console.error('DataStatus load error', e);
        if (!cancelled) setErr(e.message || 'Failed to load');
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();

    return () => { cancelled = true; };
  }, []);

  return (
    <div style={{ padding: 20, background: '#f9fafb', minHeight: '100vh' }}>
      <h1 style={{ fontSize: 22, fontWeight: 700, marginBottom: 8 }}>Data Status</h1>
      <div style={{ color: '#6b7280', marginBottom: 16 }}>
        Latest COB snapshots across datasets
      </div>

      {err && (
        <div
          style={{
            background: '#fef2f2',
            border: '1px solid #fecaca',
            color: '#991b1b',
            padding: 10,
            borderRadius: 8,
            marginBottom: 16
          }}
        >
          {err}
        </div>
      )}

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill,minmax(320px,1fr))', gap: 16 }}>
        <Box title="Valuation: Book Counts">
          {loading ? 'Loading…' : (
            <SimpleTable
              rows={valuationBooks}
              columns={[
                { key: 'book', label: 'Book' },
                { key: 'count', label: 'Count' },
              ]}
              empty="No valuation rows"
            />
          )}
        </Box>

        <Box title="Risk Shocks: Counts">
          {loading ? 'Loading…' : (
            <SimpleTable
              rows={riskShocks}
              columns={[
                { key: 'risk_factor', label: 'Risk Factor' },
                { key: 'curve', label: 'Curve' },
                { key: 'count', label: 'Count' },
              ]}
              empty="No risk shocks rows"
            />
          )}
        </Box>

        <Box title="Sensitivities: Book Counts">
          {loading ? 'Loading…' : (
            <SimpleTable
              rows={sensBooks}
              columns={[
                { key: 'book', label: 'Book' },
                { key: 'count', label: 'Count' },
              ]}
              empty="No sensitivity rows"
            />
          )}
        </Box>

        <Box title="Sensitivity PnL (by Book)">
          {loading ? 'Loading…' : (
            <SimpleTable
              rows={sensPnl}
              columns={[
                { key: 'book', label: 'Book' },
                { key: 'pnl', label: 'Total PnL' },
                { key: 'reval_pnl', label: 'Total Reval PnL' },
              ]}
              empty="No PnL rows"
            />
          )}
        </Box>

        <Box title="Valuation: Run Count">
          <div style={{ fontSize: 32, fontWeight: 700 }}>
            {loading ? '…' : (runCounts ?? '—')}
          </div>
          <div style={{ color: '#6b7280', marginTop: 4 }}>
            Distinct RUN_ID for latest COB
          </div>
        </Box>
      </div>
    </div>
  );
}
