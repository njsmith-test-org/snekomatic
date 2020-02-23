import sys
import os
import json
from glom import glom
from pathlib import Path
import pprint
import subprocess

mode = sys.argv[1]
print("Mode is:", mode)
print("working dir:", os.getcwd())
print("Kicked off by:", os.environ["GITHUB_ACTOR"])

payload = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
print("Payload:")
pprint.pprint(payload)

job_info = glom(payload, "client_payload")
print("Job info")
pprint.pprint(job_info)

subprocess.run(["ls", "-R"])

if mode == "unprivileged":
    print("making artifact")
    Path("worker-artifacts-dir").mkdir()
    Path("worker-artifacts-dir/test").write_text("hello")

    subprocess.run(["ls", "-R"])
else:
    print("reading artifact")
    print("artifact says:", Path("worker-artifacts-dir/test").read_text())
