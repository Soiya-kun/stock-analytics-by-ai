import { copyFile, mkdir, readdir, rm, stat, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const infraDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const repositoryRoot = path.resolve(infraDir, "../..");
const defaultSourceDir = path.join(repositoryRoot, "outputs");
const sourceDir = process.env.SITE_SOURCE_DIR
  ? path.resolve(process.cwd(), process.env.SITE_SOURCE_DIR)
  : defaultSourceDir;
const requestedFile = process.env.SITE_FILE
  ? path.resolve(process.cwd(), process.env.SITE_FILE)
  : undefined;

async function findHtmlFiles(directory) {
  const found = [];
  const entries = await readdir(directory, { withFileTypes: true });

  for (const entry of entries) {
    const entryPath = path.join(directory, entry.name);
    if (entry.isDirectory()) {
      found.push(...await findHtmlFiles(entryPath));
    } else if (entry.isFile() && path.extname(entry.name).toLowerCase() === ".html") {
      found.push(entryPath);
    }
  }

  return found.sort();
}

const sourceFiles = requestedFile ? [requestedFile] : await findHtmlFiles(sourceDir);
if (sourceFiles.length === 0) {
  throw new Error(`No HTML files found in ${sourceDir}`);
}

const stagingDir = path.join(infraDir, ".site-content");
await rm(stagingDir, { recursive: true, force: true });
await mkdir(stagingDir, { recursive: true });

const reports = [];
for (const sourceFile of sourceFiles) {
  const sourceStats = await stat(sourceFile);
  if (!sourceStats.isFile() || path.extname(sourceFile).toLowerCase() !== ".html") {
    throw new Error(`Not an HTML file: ${sourceFile}`);
  }

  const relativePath = requestedFile
    ? path.basename(sourceFile)
    : path.relative(sourceDir, sourceFile);
  const destination = path.join(stagingDir, relativePath);
  await mkdir(path.dirname(destination), { recursive: true });
  await copyFile(sourceFile, destination);
  reports.push({
    relativePath: relativePath.replaceAll(path.sep, "/"),
    size: sourceStats.size,
    modifiedAt: sourceStats.mtime,
  });
}

const escapeHtml = (value) => value
  .replaceAll("&", "&amp;")
  .replaceAll("<", "&lt;")
  .replaceAll(">", "&gt;")
  .replaceAll('"', "&quot;");

const items = reports
  .sort((a, b) => b.relativePath.localeCompare(a.relativePath))
  .map((report) => {
    const cleanPath = `/${report.relativePath.replace(/\.html$/i, "")}`;
    const sizeMb = (report.size / 1024 / 1024).toFixed(2);
    const modified = report.modifiedAt.toISOString().slice(0, 10);
    return `      <li><a href="${encodeURI(cleanPath)}">${escapeHtml(report.relativePath)}</a><span>${sizeMb} MB · ${modified}</span></li>`;
  })
  .join("\n");

const indexHtml = `<!doctype html>
<html lang="ja">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Stock analytics reports</title>
  <style>
    :root { color-scheme: light dark; font-family: system-ui, sans-serif; }
    body { max-width: 960px; margin: 0 auto; padding: 48px 24px; }
    h1 { margin-bottom: 8px; }
    p { color: #777; }
    ul { list-style: none; padding: 0; border-top: 1px solid #8885; }
    li { display: flex; gap: 24px; justify-content: space-between; padding: 14px 4px; border-bottom: 1px solid #8885; }
    a { overflow-wrap: anywhere; }
    span { color: #777; white-space: nowrap; }
  </style>
</head>
<body>
  <main>
    <h1>Stock analytics reports</h1>
    <p>${reports.length} HTML files</p>
    <ul>
${items}
    </ul>
  </main>
</body>
</html>
`;

await writeFile(path.join(stagingDir, "index.html"), indexHtml, "utf8");
console.log(`Prepared ${reports.length} HTML files from ${sourceDir}`);
