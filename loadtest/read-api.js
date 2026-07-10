// k6 load test for the read-api HTTP surface (GET /alerts).
// Run with the stack up:  k6 run loadtest/read-api.js
import http from "k6/http";
import { check } from "k6";

export const options = {
  vus: 50,
  duration: "30s",
  thresholds: {
    http_req_failed: ["rate<0.01"],
    http_req_duration: ["p(95)<100"],
  },
};

const BASE = __ENV.BASE_URL || "http://localhost:8090";

export default function () {
  const res = http.get(`${BASE}/alerts`);
  check(res, { "status is 200": (r) => r.status === 200 });
}
