#!/usr/bin/env node
import * as fs from "node:fs";
import * as path from "node:path";
import * as cdk from "aws-cdk-lib";
import { DailyReviewStack } from "../lib/daily-review-stack";

const root = path.resolve(__dirname, "../../..");
const staging = path.resolve(__dirname, "../.daily-content");
fs.mkdirSync(staging, { recursive: true });
// Fixed code allowlist. Never stage .env, local prices, X posts or research output.
const files: Record<string, string> = {
  "daily_stock_cloud.py": "scripts/daily_stock_cloud.py",
  "stock_review_page.py": "scripts/stock_review_page.py",
  "fetch_kabuplus_daily_csv.py": "scripts/fetch_kabuplus_daily_csv.py",
  "template.html": "site/turnover-chart-viewer/template.html",
};
for (const name of fs.readdirSync(staging)) {
  if (!(name in files)) throw new Error(`Unexpected file in Lambda staging: ${name}`);
}
for (const [name, source] of Object.entries(files)) fs.copyFileSync(path.join(root, source), path.join(staging, name));

new DailyReviewStack(new cdk.App(), "StockAnalyticsDailyReview", {
  env: { account: "373117960731", region: process.env.STACK_REGION ?? "ap-northeast-1" },
  description: "Daily KABU+ chart publication with private S3 history and Lambda execution-role authentication",
});
