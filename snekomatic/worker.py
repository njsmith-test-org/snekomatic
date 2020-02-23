import sys
import os
import json
from glom import glom
from pathlib import Path
import pprint
import subprocess

mode = sys.argv[1]
print("Mode is:", mode)
print("Kicked off by:", os.environ["GITHUB_ACTOR"])

payload = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
print("Payload:")
pprint.pprint(payload)

job_info = glom(payload, "client_payload")
print("Job info")
pprint.pprint(job_info)

subprocess.run(["ls"])
subprocess.run(["ls", "worker-artifacts-dir", "-R"])

if mode == "unprivileged":
    print("making artifact")
    Path("worker-artifacts-dir").mkdir()
    Path("worker-artifacts-dir/test").write_text("hello")

    subprocess.run(["ls"])
    subprocess.run(["ls", "worker-artifacts-dir", "-R"])
else:
    print("reading artifact")
    print("artifact says:", Path("worker-artifacts-dir/test").read_text())
