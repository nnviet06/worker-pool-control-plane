// Baseline load: a constant arrival rate of one class through the proxy.
//
//   k6 run -e RATE=10 -e DURATION=30s experiments/k6/baseline.js
//
// TARGET defaults to a proxy on 127.0.0.1:8080. Served and rejected requests
// are reported as separate trends, so the latency of admitted requests is
// not mixed with fast rejections.
import http from 'k6/http';
import { Counter, Trend } from 'k6/metrics';

const target = __ENV.TARGET || 'http://127.0.0.1:8080';

const served = new Trend('served_duration', true);
const rejected = new Trend('rejected_duration', true);
const other = new Counter('other_status');

export const options = {
  scenarios: {
    steady: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RATE || 10),
      timeUnit: '1s',
      duration: __ENV.DURATION || '30s',
      preAllocatedVUs: 50,
    },
  },
  summaryTrendStats: ['count', 'med', 'p(99)', 'max'],
};

export default function () {
  const res = http.post(`${target}/`, 'payload', { headers: { 'X-Wpcp-Class': 'default' } });
  if (res.status === 200) {
    served.add(res.timings.duration);
  } else if (res.status === 503) {
    rejected.add(res.timings.duration);
  } else {
    other.add(1);
  }
}
