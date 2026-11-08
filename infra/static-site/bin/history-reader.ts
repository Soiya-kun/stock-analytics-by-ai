#!/usr/bin/env node
import * as fs from 'node:fs';
import * as path from 'node:path';
import { createHash } from 'node:crypto';
import * as cdk from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as cloudfront from 'aws-cdk-lib/aws-cloudfront';
import * as s3deploy from 'aws-cdk-lib/aws-s3-deployment';

const infraRoot = path.resolve(__dirname, '..');
const sourceDir = path.resolve(infraRoot, '../../outputs/stock-history-reader/285A');
const manifest = JSON.parse(fs.readFileSync(path.join(sourceDir, 'publish.json'), 'utf8'));
const prefix = 'market-history/kioxia';
if (manifest.schema_version !== 1 || manifest.sc !== '285A' || manifest.prefix !== prefix ||
    JSON.stringify(Object.keys(manifest.files)) !== JSON.stringify(['index.html'])) {
  throw new Error('Unexpected history reader public manifest');
}
const html = fs.readFileSync(path.join(sourceDir, 'index.html'));
const digest = createHash('sha256').update(html).digest('hex');
if (manifest.files['index.html'] !== digest) throw new Error('History HTML checksum mismatch');
// Stage only the reviewed, self-contained HTML. Never package its parent directory.
const staging = path.join(infraRoot, '.reader-content', digest);
fs.mkdirSync(staging, {recursive: true});
if (fs.readdirSync(staging).some(name => name !== 'index.html')) throw new Error('Unexpected staged asset');
fs.writeFileSync(path.join(staging, 'index.html'), html);

const app = new cdk.App();
const stack = new cdk.Stack(app, 'StockAnalyticsHistoryReader285A', {
  env: {account: '373117960731', region: process.env.STACK_REGION ?? 'ap-northeast-1'},
  description: 'Publish the scroll-linked Kioxia history reader to a separate existing-site prefix',
});
const bucketName = new cdk.CfnParameter(stack, 'SiteBucketName', {type: 'String'});
const distributionId = new cdk.CfnParameter(stack, 'SiteDistributionId', {type: 'String'});
const domainName = new cdk.CfnParameter(stack, 'SiteDomainName', {type: 'String'});
const bucket = s3.Bucket.fromBucketName(stack, 'ExistingSiteBucket', bucketName.valueAsString);
const distribution = cloudfront.Distribution.fromDistributionAttributes(stack, 'ExistingDistribution', {
  distributionId: distributionId.valueAsString, domainName: domainName.valueAsString,
});
new s3deploy.BucketDeployment(stack, 'PublishReader', {
  sources: [s3deploy.Source.asset(staging)], destinationBucket: bucket,
  destinationKeyPrefix: prefix, distribution, distributionPaths: [`/${prefix}/*`],
  prune: false, retainOnDelete: true, memoryLimit: 1024,
});
new cdk.CfnOutput(stack, 'ReportUrl', {value: `https://${domainName.valueAsString}/${prefix}/`});
new cdk.CfnOutput(stack, 'ReportSha256', {value: digest});
