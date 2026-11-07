#!/usr/bin/env node
import * as cdk from "aws-cdk-lib";
import { StaticSiteStack } from "../lib/static-site-stack";

const app = new cdk.App();

new StaticSiteStack(app, "StockAnalyticsStaticSite", {
  env: {
    account: "373117960731",
    region: process.env.STACK_REGION ?? "ap-northeast-1",
  },
  description: "Private S3 origin and CloudFront distribution for stock analysis HTML reports",
});
