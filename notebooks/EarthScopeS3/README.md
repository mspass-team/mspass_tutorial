# EarthScope S3 daily waveform workflow

This directory adapts the collaborator-supplied `reexternal2014datastatus.zip`
notebooks and Python modules (August 2026). Original module authorship is retained.
It belongs in the tutorial repository, not the MsPASS library. It is a contributed
workflow requiring local data and authorization, **not a validated GeoLab
production recipe**. No notebook cells have been executed against the supplied
CSS data or live GeoLab/S3 as part of this change.

## What is fixed

- `S3Worker` uses the EarthScope SDK's refreshable boto3 session directly. It
  does not freeze temporary credentials or fabricate a new expiration for cached
  credentials. One SDK/client pair lives until worker-plugin teardown; partial
  setup is cleaned up. The client is a named worker-plugin resource, not spillable
  `worker.data` or a task argument.
- Read/list/authentication/transport failures retain their original exception.
  Only a definite missing-object HEAD response permits `#N` version fallback.
  A generic HEAD 403 does not prove that the object exists. Missing/undecodable
  waveforms still produce an error log; systemic failures stop processing.
- S3 response bodies close on success and failure. Upload errors, including an
  error on file close/commit, propagate instead of returning an ignored `False`.
- Completion consumes its disposable input list as it merges station ensembles,
  preserving member order and values while releasing source ensembles before
  pickling. The native member vector is pre-reserved. This does not eliminate
  native pickle's safe snapshot copies or guarantee a maximum RSS.
- Index listing is paginated. Day groups use the union of required days, and an
  arrival is emitted once rather than once per joined holding. Year filters are
  half-open and the index includes neighboring days needed by padded windows.
- Multi-rate data are converted once; requested detrending acts on the decoded
  data. The metadata loader preserves unmatched station names and excludes `AK`
  by value, not by the order MongoDB returns network codes.
- Saved notebook outputs, credential-printing cells and personal scratch paths
  are removed. Metadata export streams a JSON array instead of loading the entire
  collection into a second list.

## What is deliberately unchanged

Output is **one ordinary `TimeSeriesEnsemble` pickle per day**, not a new stream
format or serialization option. Normal keys remain
`<SCRATCH_BUCKET>/<year>/<year>_<julian-day>.pickle`, including the complete user
prefix from `SCRATCH_BUCKET`. Existing readers using `pickle.load` keep working
with a compatible MsPASS installation. A waveform may still require a large
whole-day worker result, a driver gather, native copies, and upload buffering.

The input access point and `s3-miniseed` SDK role are retained from the supplied
workflow. This is not an automatic migration to EarthScope's newer S3 interfaces.
Confirm that this legacy endpoint and your intended networks remain available
and authorized. See the [EarthScope S3 documentation](https://docs.earthscope.org/sdk/s3-direct-access-tutorial)
for current access rules. A role/bucket migration needs its own validation.

The reader plugin and output `s3fs.S3FileSystem()` have **separate credential
providers**. SDK refresh on the reader does not renew copied/static credentials
used by the output filesystem, and does not grant scratch write permission.

## Prerequisites and setup

Use a dedicated working directory and database. The default database name in the
notebooks is `ANF48`; change it consistently if this is not a disposable test
database. Initialization drops/replaces `source`, `netmag` and `wf_s3` collections
and may duplicate arrivals on repeated runs. Do not run it on a valuable existing
database or rerun all initialization cells as a recovery step.

1. Install a compatible current MsPASS build in the notebook **and workers**.
   Required behavior includes `sliding_window_pipeline(..., retain_results=False)`
   and the native ensemble pickle fixes through mspass-team/mspass#1025.
   Updating Python source does not rebuild an already loaded native extension.
2. In that environment install `python -m pip install -r requirements.txt`.
   The SDK range covers the session API used/tested here. Resolve s3fs/aiobotocore
   dependencies together in a clean environment; do not overwrite a running
   shared environment. pandas, ObsPy, PyMongo, Dask and boto3 are also required.
3. Authenticate EarthScope using the supported GeoLab/SDK mechanism on every
   worker. Separately configure a refreshable identity authorized to write your
   GeoLab scratch prefix. Never put keys/tokens into task arguments or notebook
   output. No authentication setup is performed by the tests.
4. Obtain the CSS/Antelope tables separately, including the required `snetsta`
   mapping, and place the supplied `usarray48.*` tables under `css30/`. The large
   tables are not distributed here. Do not assume the public bulletin archive
   contains the complete station cross-reference table used by the contributor.
5. Copy `data/pf/DatascopeDatabase.pf` from your MsPASS distribution into
   `pf/DatascopeDatabase.pf`, as expected by `load_anfdata.ipynb`.
6. Open the notebooks with this directory as the working directory, so module
   imports and `dask_client.upload_file(...)` find the adjacent Python files.

## Notebook order

1. `load_anfdata.ipynb`: load the CSS catalog and retrieve station metadata.
   Review the contributor's geographic/phase selection before accepting it.
2. `s3indexing_2014.ipynb`: set `year` and build that year's index plus adjacent
   days. Review `BUCKET` if your supported input endpoint differs.
3. `load_waveforms_s3_2014.ipynb`: set the same `year`/`BUCKET`, then choose a
   populated subset with `first_julday` and `days_to_process`. The initial defaults
   process three days with a sliding window of **1**. Missing holdings are exposed
   before task submission. The scalar accumulator reports successful day files;
   an empty/dead day is not a successful write.
4. Optional: `ExportMetadata.ipynb` exports metadata as JSON arrays;
   `SetupDataTransfer.ipynb` describes copying output with an independently
   authorized CLI, without printing credentials.

`save_jday_outputs` runs on the driver, so `s3_day_workflow.py` stays there.
The notebook uploads the reader/plugin modules to workers. Do not enable
`completion_on_worker=True` with live database/filesystem handles as task
arguments; worker-side saving is a separate workflow change, not required here.

Restart the kernel/workers when replacing modules from the old ZIP, then register
`S3Worker()` again. Its default registration name is `s3client`; with a custom
`S3Worker(key="archive")`, readers must use `fetch_s3_client(worker_data_key="archive")`.
Fetch no longer looks in `worker.data`. Do not override the registration name
independently of the key. Unregister the plugin/close the cluster when finished.

If a systemic read/write error stops processing, already completed files and
failure records are not rolled back. Check actual output keys and read failures
before resuming. Tagged day outputs use the same keys on rerun and may overwrite
previous objects. This workflow is not a transactional or automatic resume system.

## Offline checks

From this directory in a compatible MsPASS environment:

```bash
python -m pip install pytest
python -m pytest -q test_s3_worker_plugin.py test_notebooks.py test_plugin_process.py test_workflow.py
```

The credential tests use the real SDK session builder and botocore, with a fake
credential endpoint/cache and a controllable clock. They exercise a cached token
at minutes 41/55 and refresh beyond minutes 60/120, near-expired initial tokens,
refresh failures, and resource cleanup. They do not contact AWS or log keys.
The separate-process Dask probe uses an intentionally unpickleable fake client to
verify registration, lookup outside task storage, and teardown.

The native tests use real ObsPy miniSEED and MsPASS objects with fake S3/database
boundaries. They check pagination, cross-year grouping, error propagation, body
cleanup, conversion/detrending, member order and sample values, ordinary pickle
round trips, and weak references **before serialization**, not just after return.
The Dask probe has a 45-second timeout and cleans up its process group.

For a small memory comparison, run each mode in a fresh process (requires psutil):

```bash
python benchmark_completion_memory.py legacy
python benchmark_completion_memory.py fixed
```

A local Python 3.13/protocol-4 run with 64 MiB of samples and the #1025 native
pickle implementation measured approximately 323 MiB versus 259 MiB RSS above
baseline during serialization. Both produced 64.16 MiB pickle streams. This is a
synthetic, no-S3 comparison, not a prediction for GeoLab or a peak-memory bound.

GitHub Actions runs only credential, notebook, and worker-process checks on
Python 3.10/3.13; it does not install/build native MsPASS. `test_workflow.py` must
also run in a compatible native environment. No fake-backed test proves that
live authentication, scratch policies or a complete year's data work correctly.

## Required GeoLab acceptance checks

- First run `import os; print(os.getpid())` in the kernel and correlate it with
  `ps`/`top`; do not infer the driver solely from a command named `python`.
- Verify loaded native behavior on the driver and workers. For example,
  `type(TimeSeriesEnsemble().__getstate__()[3]).__name__` should be `list`, not
  legacy `bytes`. This checks #1025 behavior, not every installed commit.
- Run a low-volume authentication check across the *real* token expiration
  (e.g. 75–90 minutes), including the input and output providers independently.
  Preserve operation, Error.Code, HTTP status and RequestId on the first failure;
  never include secret keys, session tokens or signed headers in a report.
- Run a small day, representative day and heavy day. Record driver RSS before
  building request lists, at completion entry, after merge and after upload.
  Check actual object counts, member counts and samples with downstream readers.
- Increase concurrency only after measuring total pod memory. A window bounds
  task count, not bytes. `del ens` immediately before return or forced garbage
  collection is not a substitute for reducing live copies, task size and
  allocator high-water usage.

Full CSS/GeoLab execution, live credential renewal, real scratch upload throughput
and the collaborator's 12–18 GiB RSS remain explicitly unverified here.
