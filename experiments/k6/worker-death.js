// Worker death under steady load: one worker is stopped with POST /die
// partway through the run.
//
//   k6 run -e RATE=10 -e DURATION=30s -e DIE_AT=10s experiments/k6/worker-death.js
//
// TARGET is the proxy, DIE_URL the admin address of the worker to stop.
// SAFE=false sends requests without X-Wpcp-Safe-To-Failover, for comparison.
import http from 'k6/http';
import { Rate, Trend } from 'k6/metrics';

const target = __ENV.TARGET || 'http://127.0.0.1:8080';
const dieURL = __ENV.DIE_URL || 'http://127.0.0.1:8081';
const safe = __ENV.SAFE !== 'false';

const success = new Rate('success');
const served = new Trend('served_duration', true);

export const options = {
  scenarios: {
    load: {
      executor: 'constant-arrival-rate',
      exec: 'load',
      rate: Number(__ENV.RATE || 10),
      timeUnit: '1s',
      duration: __ENV.DURATION || '30s',
      preAllocatedVUs: 50,
    },
    fault: {
      executor: 'shared-iterations',
      exec: 'fault',
      iterations: 1,
      vus: 1,
      startTime: __ENV.DIE_AT || '10s',
    },
  },
  summaryTrendStats: ['count', 'med', 'p(99)', 'max'],
};

export function load() {
  const headers = { 'X-Wpcp-Class': 'default' };
  if (safe) {
    headers['X-Wpcp-Safe-To-Failover'] = 'true';
  }
  const res = http.post(`${target}/`, 'payload', { headers });
  success.add(res.status === 200);
  if (res.status === 200) {
    served.add(res.timings.duration);
  }
}

export function fault() {
  http.post(`${dieURL}/die`);
}
