"""Deploy, seed, verify and then enable the daily Lambda. Initial operator SSO only."""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
INFRA = ROOT / "infra/static-site"
STACK = "StockAnalyticsDailyReview"
ACCOUNT = "373117960731"


def run(parts, *, cwd=ROOT, timeout=900, show=False):
    result = subprocess.run(parts, cwd=cwd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout,
                            env={**os.environ, "AWS_PAGER": "", "AWS_CLI_AUTO_PROMPT": "off"})
    if show:
        print(result.stdout, end="", flush=True)
        print(result.stderr, end="", flush=True)
    if result.returncode:
        raise RuntimeError(f"{Path(parts[0]).name} failed, exit {result.returncode}")
    return result.stdout


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed-dir", type=Path, required=True)
    parser.add_argument("--profile", default="373117960731_AdministratorAccess")
    parser.add_argument("--region", default="ap-northeast-1")
    args = parser.parse_args()
    os.environ["STACK_REGION"] = args.region
    seed = args.seed_dir.resolve()
    summary = json.loads((seed / "seed.json").read_text(encoding="utf-8"))
    state = seed / "prices.sqlite.gz"
    if hashlib.sha256(state.read_bytes()).hexdigest() != summary["state_sha256"]:
        raise ValueError("Seed checksum mismatch")

    def aws(*parts, timeout=120):
        return run(["aws", *parts, "--profile", args.profile, "--region", args.region, "--output", "json"], timeout=timeout)

    if json.loads(aws("sts", "get-caller-identity"))["Account"] != ACCOUNT:
        raise ValueError("Unexpected AWS account")
    stack = json.loads(aws("cloudformation", "describe-stacks", "--stack-name", "StockAnalyticsStaticSite"))["Stacks"][0]
    outputs = {x["OutputKey"]: x["OutputValue"] for x in stack["Outputs"]}
    resources = json.loads(aws("cloudformation", "list-stack-resources", "--stack-name", "StockAnalyticsStaticSite"))["StackResourceSummaries"]
    distributions = [x["PhysicalResourceId"] for x in resources if x["ResourceType"] == "AWS::CloudFront::Distribution"]
    if len(distributions) != 1:
        raise ValueError("Ambiguous existing CloudFront distribution")
    npx = shutil.which("npx.cmd" if os.name == "nt" else "npx")
    npm = shutil.which("npm.cmd" if os.name == "nt" else "npm")
    run([npm, "run", "build"], cwd=INFRA, show=True)
    run([npx, "cdk", "synth", "--app", "npx ts-node --prefer-ts-exts bin/daily-review.ts", "--output", "cdk.daily.out", "--profile", args.profile, "--quiet"], cwd=INFRA, show=True)
    parameters = ["--parameters", f"SiteBucketName={outputs['BucketName']}", "--parameters", f"SiteDistributionId={distributions[0]}", "--parameters", f"SiteUrl={outputs['SiteUrl']}"]
    run([npx, "cdk", "diff", "--app", "cdk.daily.out", "--profile", args.profile, "--no-change-set"], cwd=INFRA, show=True)

    def deploy(enabled):
        run([npx, "cdk", "deploy", "--app", "cdk.daily.out", "--profile", args.profile,
             "--require-approval", "never", "--parameters", f"SchedulesEnabled={'true' if enabled else 'false'}",
             "--outputs-file", "cdk.daily.out/deployed-outputs.json", *parameters], cwd=INFRA, timeout=1800, show=True)
        return json.loads((INFRA / "cdk.daily.out/deployed-outputs.json").read_text(encoding="utf-8"))[STACK]

    print("Deploying with schedules disabled until seed and live verification succeed", flush=True)
    deployed = deploy(False)
    existing = json.loads(aws("s3api", "list-objects-v2", "--bucket", deployed["DataBucketName"], "--prefix", "state/prices.sqlite.gz", "--max-keys", "1"))
    if any(item["Key"] == "state/prices.sqlite.gz" for item in existing.get("Contents", [])):
        current = json.loads(aws("s3api", "head-object", "--bucket", deployed["DataBucketName"], "--key", "state/prices.sqlite.gz")).get("Metadata", {}).get("trading-date")
        if current and current > summary["target_date"]:
            raise ValueError("Cloud history is newer than the local seed; update the local DB before reseeding")
    # Capture Docker-resolved credentials in memory. Never print them, pass them
    # in command-line arguments, or include them in CDK assets/configuration.
    raw = run([
        "docker", "compose", "run", "--rm", "-T", "-e", "CSVEX_BASIC_USER", "-e", "CSVEX_BASIC_PASSWORD",
        "--entrypoint", "python", "importer", "-c",
        "import json,os; print(json.dumps({'username':os.environ.get('CSVEX_BASIC_USER'),'password':os.environ.get('CSVEX_BASIC_PASSWORD')}))",
    ])
    credentials = json.loads(raw)
    if not credentials["username"] or not credentials["password"]:
        raise ValueError("CSVEX credentials are missing from Docker environment")
    private_temp = ROOT / ".secrets"
    private_temp.mkdir(exist_ok=True)
    if not private_temp.resolve().is_relative_to(ROOT.resolve()):
        raise ValueError("Secret staging directory must remain inside the repository")
    with tempfile.TemporaryDirectory(prefix="daily-deploy-", dir=private_temp) as temporary:
        credential_path = Path(temporary) / "credentials.json"
        credential_path.write_text(json.dumps(credentials), encoding="utf-8")
        aws("secretsmanager", "put-secret-value", "--secret-id", deployed["CsvSecretArn"], "--secret-string", f"file://{credential_path}")
    del credentials, raw
    aws("s3", "cp", str(state), f"s3://{deployed['DataBucketName']}/state/prices.sqlite.gz", "--only-show-errors",
        "--content-type", "application/gzip", "--metadata", f"trading-date={summary['target_date']},sha256={summary['state_sha256']}")
    aws("s3", "cp", str(seed / "seed.json"), f"s3://{deployed['DataBucketName']}/state/seed.json", "--only-show-errors")
    print("Invoking Lambda with execution-role credentials and real KABU+ acquisition", flush=True)
    payload = seed / "invoke.json"
    payload.write_text(json.dumps({"target_date": summary["target_date"], "force_refresh": True}), encoding="utf-8")
    result_file = seed / "lambda-result.json"
    metadata = json.loads(aws("lambda", "invoke", "--function-name", deployed["FunctionName"],
        "--cli-connect-timeout", "15", "--cli-read-timeout", "900", "--payload", f"fileb://{payload}", str(result_file), timeout=960))
    result = json.loads(result_file.read_text(encoding="utf-8"))
    print(json.dumps({"invoke": metadata, "result": result}, ensure_ascii=False), flush=True)
    if metadata.get("FunctionError") or result.get("status") != "published":
        raise RuntimeError("Lambda live verification failed; schedules remain disabled")

    curl = shutil.which("curl.exe" if os.name == "nt" else "curl")
    public = seed / "public.html"
    run([curl, "-4", "--fail", "--silent", "--show-error", "--connect-timeout", "10", "--max-time", "45", "--output", str(public), result["url"]], timeout=60)
    if hashlib.sha256(public.read_bytes()).hexdigest() != result["sha256"]:
        raise ValueError("Independent public HTTPS verification failed")
    print("Public HTTPS verified. Enabling weekday schedules", flush=True)
    deploy(True)
    record = {"resources": deployed, "verification": result, "seed": summary, "schedules_enabled": True}
    (seed / "deployment.json").write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"status": "deployed_and_verified", "url": result["url"], "target_date": result["target_date"]}, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
