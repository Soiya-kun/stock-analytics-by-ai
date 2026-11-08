import * as path from "node:path";
import * as cdk from "aws-cdk-lib";
import * as cloudwatch from "aws-cdk-lib/aws-cloudwatch";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as logs from "aws-cdk-lib/aws-logs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as scheduler from "aws-cdk-lib/aws-scheduler";
import * as secretsmanager from "aws-cdk-lib/aws-secretsmanager";
import * as sqs from "aws-cdk-lib/aws-sqs";
import { Construct } from "constructs";

export class DailyReviewStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);
    const siteBucket = new cdk.CfnParameter(this, "SiteBucketName", { type: "String" });
    const distribution = new cdk.CfnParameter(this, "SiteDistributionId", { type: "String" });
    const siteUrl = new cdk.CfnParameter(this, "SiteUrl", { type: "String" });
    const enabled = new cdk.CfnParameter(this, "SchedulesEnabled", { type: "String", default: "false", allowedValues: ["true", "false"] });
    const enabledCondition = new cdk.CfnCondition(this, "EnableSchedules", { expression: cdk.Fn.conditionEquals(enabled, "true") });

    const data = new s3.Bucket(this, "PriceHistory", {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL, encryption: s3.BucketEncryption.S3_MANAGED,
      enforceSSL: true, versioned: true, removalPolicy: cdk.RemovalPolicy.RETAIN,
      lifecycleRules: [
        { prefix: "state/", noncurrentVersionExpiration: cdk.Duration.days(30), noncurrentVersionsToRetain: 3 },
        { prefix: "control/", noncurrentVersionExpiration: cdk.Duration.days(30), noncurrentVersionsToRetain: 2 },
        { prefix: "status/runs/", expiration: cdk.Duration.days(90) },
      ],
    });
    const secret = new secretsmanager.Secret(this, "CsvCredentials", {
      description: "KABU+ CSV Basic credentials; populated by the local deployment script, never CloudFormation",
    });
    secret.applyRemovalPolicy(cdk.RemovalPolicy.RETAIN);
    const logGroup = new logs.LogGroup(this, "DailyLogs", { retention: logs.RetentionDays.TWO_WEEKS, removalPolicy: cdk.RemovalPolicy.RETAIN });
    const deadLetters = new sqs.Queue(this, "FailedEvents", { retentionPeriod: cdk.Duration.days(14), enforceSSL: true });
    const fn = new lambda.Function(this, "DailyReview", {
      functionName: "stock-analytics-daily-review", runtime: lambda.Runtime.PYTHON_3_12,
      architecture: lambda.Architecture.ARM_64, handler: "daily_stock_cloud.handler",
      code: lambda.Code.fromAsset(path.join(__dirname, "../.daily-content")),
      memorySize: 1536, ephemeralStorageSize: cdk.Size.gibibytes(1), timeout: cdk.Duration.minutes(15),
      logGroup,
      environment: { DATA_BUCKET: data.bucketName, SITE_BUCKET: siteBucket.valueAsString,
        SITE_URL: siteUrl.valueAsString, DISTRIBUTION_ID: distribution.valueAsString, CSVEX_SECRET_ARN: secret.secretArn },
      deadLetterQueue: deadLetters, retryAttempts: 1, maxEventAge: cdk.Duration.hours(1),
    });
    secret.grantRead(fn);
    fn.addToRolePolicy(new iam.PolicyStatement({ actions: ["s3:GetObject", "s3:PutObject"],
      resources: ["state/*", "raw/daily/*", "status/*", "control/*"].map(prefix => data.arnForObjects(prefix)) }));
    // S3 needs ListBucket permission to return 404 (rather than 403) for a
    // not-yet-created lock/status object. This is the private price bucket only.
    fn.addToRolePolicy(new iam.PolicyStatement({ actions: ["s3:ListBucket"], resources: [data.bucketArn] }));
    fn.addToRolePolicy(new iam.PolicyStatement({ actions: ["s3:GetObject", "s3:PutObject"],
      resources: [`arn:${this.partition}:s3:::${siteBucket.valueAsString}/daily/index.html`] }));
    fn.addToRolePolicy(new iam.PolicyStatement({ actions: ["cloudfront:CreateInvalidation", "cloudfront:GetInvalidation"],
      resources: [`arn:${this.partition}:cloudfront::${this.account}:distribution/${distribution.valueAsString}`] }));

    const schedulerRole = new iam.Role(this, "SchedulerRole", { assumedBy: new iam.ServicePrincipal("scheduler.amazonaws.com") });
    fn.grantInvoke(schedulerRole);
    deadLetters.grantSendMessages(schedulerRole);
    for (const [label, expression] of Object.entries({
      Main: "cron(40 16 ? * MON-FRI *)", Retry1: "cron(10 17 ? * MON-FRI *)", Retry2: "cron(10 18 ? * MON-FRI *)",
    })) {
      new scheduler.CfnSchedule(this, label, {
        name: `stock-analytics-daily-${label.toLowerCase()}`, scheduleExpression: expression,
        scheduleExpressionTimezone: "Asia/Tokyo", flexibleTimeWindow: { mode: "OFF" },
        state: cdk.Fn.conditionIf(enabledCondition.logicalId, "ENABLED", "DISABLED").toString(),
        target: { arn: fn.functionArn, roleArn: schedulerRole.roleArn, input: "{}",
          retryPolicy: { maximumEventAgeInSeconds: 3600, maximumRetryAttempts: 2 },
          deadLetterConfig: { arn: deadLetters.queueArn } },
      });
    }
    new cloudwatch.Alarm(this, "DailyFailure", { metric: fn.metricErrors({ period: cdk.Duration.hours(1) }),
      threshold: 1, evaluationPeriods: 1, treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
      alarmDescription: "Daily stock Lambda failed. Inspect its log group and private status/latest-run.json." });

    new cdk.CfnOutput(this, "FunctionName", { value: fn.functionName });
    new cdk.CfnOutput(this, "DataBucketName", { value: data.bucketName });
    new cdk.CfnOutput(this, "CsvSecretArn", { value: secret.secretArn });
    new cdk.CfnOutput(this, "DailyUrl", { value: `${siteUrl.valueAsString}/daily/` });
    new cdk.CfnOutput(this, "LogGroupName", { value: logGroup.logGroupName });
  }
}
